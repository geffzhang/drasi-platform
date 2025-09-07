// Copyright 2024 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Threading.Channels;
using Confluent.Kafka;
using Drasi.Source.SDK;
using Drasi.Source.SDK.Models;
using System.Diagnostics;

namespace Reactivator.Services
{
    class TopicConsumer : BackgroundService
    {
        private readonly Channel<SourceChange> _channel;
        private readonly IStateStore _checkpointStore;
        private readonly IEventMapper _eventMapper;
        private readonly IConfiguration _configuration;
        private readonly IConsumer<string, string> _client;
        private readonly string _topicName;
        private readonly ILogger _logger;
        private readonly RetryPolicy _retryPolicy;
        private readonly KafkaMetrics _metrics;

        public TopicConsumer(Channel<SourceChange> channel, IStateStore stateStore, IEventMapper eventMapper, IConfiguration configuration, ILogger logger, string topicName)
        {
            _channel = channel;
            _checkpointStore = stateStore;
            _eventMapper = eventMapper;
            _configuration = configuration;
            _topicName = topicName;
            _logger = logger;
            _client = BuildClient(configuration, logger);
            _retryPolicy = new RetryPolicy(logger, 
                maxRetries: configuration.GetValue<int>("maxRetries", 10),
                initialDelayMs: configuration.GetValue<int>("initialRetryDelayMs", 1000),
                maxDelayMs: configuration.GetValue<int>("maxRetryDelayMs", 60000));
            _metrics = KafkaMetrics.Instance;
        }

        internal static IConsumer<string, string> BuildClient(IConfiguration configuration, ILogger logger)
        {
            var groupId = configuration.GetValue<string>("groupId") ?? "drasi-kafka-source";
            
            var config = new ConsumerConfig
            {
                GroupId = groupId,
                BootstrapServers = configuration.GetValue<string>("bootstrapServers"),
                AutoOffsetReset = configuration.GetValue<AutoOffsetReset>("autoOffsetReset", AutoOffsetReset.Latest),
                EnableAutoCommit = configuration.GetValue<bool>("enableAutoCommit", false),
                SessionTimeoutMs = configuration.GetValue<int>("sessionTimeoutMs", 6000),
                HeartbeatIntervalMs = configuration.GetValue<int>("heartbeatIntervalMs", 3000),
                MaxPollIntervalMs = configuration.GetValue<int>("maxPollIntervalMs", 300000),
                FetchMaxBytes = configuration.GetValue<int>("fetchMaxBytes", 1048576),
                FetchMinBytes = configuration.GetValue<int>("fetchMinBytes", 1),
                FetchMaxWaitMs = configuration.GetValue<int>("fetchMaxWaitMs", 500),
                StatisticsIntervalMs = configuration.GetValue<int>("statisticsIntervalMs", 5000)
            };

            // Add authentication if provided
            var username = configuration.GetValue<string>("username");
            var password = configuration.GetValue<string>("password");
            
            if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password))
            {
                config.SecurityProtocol = SecurityProtocol.SaslPlaintext;
                config.SaslMechanism = SaslMechanism.Plain;
                config.SaslUsername = username;
                config.SaslPassword = password;
            }

            logger.LogInformation("Creating Kafka consumer with group ID: {GroupId}", groupId);
            return new ConsumerBuilder<string, string>(config).Build();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Starting Kafka consumer for topic: {Topic}", _topicName);

            try
            {
                // Register statistics handler if enabled
                if (_client.Config.StatisticsIntervalMs > 0)
                {
                    _client.Statistics += (_, stats) => 
                    {
                        _metrics.UpdateStatistics(_topicName, stats);
                    };
                }

                // Subscribe to the topic
                await _retryPolicy.ExecuteWithRetryAsync(
                    async () => 
                    {
                        _client.Subscribe(_topicName);
                        await Task.CompletedTask;
                    },
                    $"Subscribe to topic {_topicName}",
                    stoppingToken
                );

                // Get saved offsets from state store and seek to them
                await RestoreOffsets(stoppingToken);

                while (!stoppingToken.IsCancellationRequested)
                {
                    ConsumeResult<string, string>? consumeResult = null;
                    
                    try
                    {
                        // Use stopwatch to measure processing time
                        var stopwatch = Stopwatch.StartNew();
                        
                        // Consume message with retry policy
                        await _retryPolicy.ExecuteWithRetryAsync(
                            async () => 
                            {
                                consumeResult = _client.Consume(TimeSpan.FromSeconds(5));
                                await Task.CompletedTask;
                            },
                            $"Consume from topic {_topicName}",
                            stoppingToken
                        );
                        
                        if (consumeResult == null)
                            continue;

                        // Record message lag
                        var messageLag = DateTimeOffset.UtcNow - 
                            (consumeResult.Message.Timestamp.Type == TimestampType.CreateTime 
                                ? DateTimeOffset.FromUnixTimeMilliseconds(consumeResult.Message.Timestamp.UnixTimestampMs)
                                : DateTimeOffset.UtcNow);
                        
                        _metrics.RecordMessageLag(_topicName, messageLag);

                        long reactivatorStartNs = (DateTimeOffset.UtcNow.Ticks - DateTimeOffset.UnixEpoch.Ticks) * 100;
                        
                        // Process the message
                        var change = await _eventMapper.MapEventAsync(consumeResult, reactivatorStartNs);
                        
                        await _channel.Writer.WriteAsync(change, stoppingToken);
                        
                        // Save offset to state store
                        await SaveOffset(consumeResult, stoppingToken);
                        
                        // Record processing time
                        stopwatch.Stop();
                        _metrics.RecordProcessingTime(_topicName, stopwatch.Elapsed);
                        
                        _logger.LogDebug("Published change for topic {Topic}, partition {Partition}, offset {Offset}, lag {Lag}ms", 
                            consumeResult.Topic, consumeResult.Partition.Value, consumeResult.Offset.Value, messageLag.TotalMilliseconds);
                    }
                    catch (TaskCanceledException)
                    {
                        _logger.LogInformation("Shutting down topic consumer: {Topic}", _topicName);
                        break;
                    }
                    catch (Exception ex)
                    {
                        _metrics.RecordError(_topicName);
                        _logger.LogError(ex, "Unhandled error in consumer loop for topic {Topic}: {Message}", _topicName, ex.Message);
                        
                        // Brief delay before continuing the loop
                        await Task.Delay(1000, stoppingToken);
                    }
                }
            }
            finally
            {
                try
                {
                    _client.Close();
                    _logger.LogInformation("Kafka consumer for topic {Topic} has been stopped", _topicName);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error closing Kafka consumer for topic {Topic}", _topicName);
                }
            }
        }

        private async Task RestoreOffsets(CancellationToken cancellationToken)
        {
            await _retryPolicy.ExecuteWithRetryAsync(
                async () =>
                {
                    var assignment = _client.Assignment;
                    var offsetsToSeek = new List<TopicPartitionOffset>();

                    // Get partition assignment (might need to wait for it)
                    var retries = 0;
                    while (!assignment.Any() && retries < 10)
                    {
                        await Task.Delay(1000, cancellationToken);
                        assignment = _client.Assignment;
                        retries++;
                    }

                    if (!assignment.Any())
                    {
                        throw new InvalidOperationException($"No partitions assigned for topic {_topicName} after waiting");
                    }

                    _logger.LogInformation("Assigned {PartitionCount} partitions for topic {Topic}", 
                        assignment.Count, _topicName);

                    foreach (var partition in assignment)
                    {
                        var stateKey = $"{_topicName}-{partition.Partition}";
                        var savedOffsetBytes = await _checkpointStore.Get(stateKey);
                        
                        if (savedOffsetBytes != null)
                        {
                            var savedOffset = BitConverter.ToInt64(savedOffsetBytes);
                            offsetsToSeek.Add(new TopicPartitionOffset(partition, new Offset(savedOffset + 1)));
                            _logger.LogInformation("Resuming from offset {Offset} for topic {Topic}, partition {Partition}", 
                                savedOffset + 1, _topicName, partition.Partition);
                        }
                        else
                        {
                            _logger.LogInformation("No saved offset found for topic {Topic}, partition {Partition}. " +
                                                "Will use {DefaultBehavior} as configured.", 
                                _topicName, partition.Partition, _client.Config.AutoOffsetReset);
                        }
                    }

                    if (offsetsToSeek.Any())
                    {
                        foreach (var offset in offsetsToSeek)
                        {
                            _client.Seek(offset);
                        }
                    }
                },
                $"Restore offsets for topic {_topicName}",
                cancellationToken
            );
        }

        private async Task SaveOffset(ConsumeResult<string, string> consumeResult, CancellationToken cancellationToken)
        {
            await _retryPolicy.ExecuteWithRetryAsync(
                async () =>
                {
                    var stateKey = $"{_topicName}-{consumeResult.Partition}";
                    var offsetBytes = BitConverter.GetBytes(consumeResult.Offset.Value);
                    await _checkpointStore.Put(stateKey, offsetBytes);
                },
                $"Save offset for topic {_topicName}, partition {consumeResult.Partition.Value}, offset {consumeResult.Offset.Value}",
                cancellationToken
            );
        }

        public override void Dispose()
        {
            _client?.Dispose();
            base.Dispose();
        }
    }
}