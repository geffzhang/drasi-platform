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

        public TopicConsumer(Channel<SourceChange> channel, IStateStore stateStore, IEventMapper eventMapper, IConfiguration configuration, ILogger logger, string topicName)
        {
            _channel = channel;
            _checkpointStore = stateStore;
            _eventMapper = eventMapper;
            _configuration = configuration;
            _topicName = topicName;
            _logger = logger;
            _client = BuildClient(configuration, logger);
        }

        internal static IConsumer<string, string> BuildClient(IConfiguration configuration, ILogger logger)
        {
            var groupId = configuration.GetValue<string>("groupId") ?? "drasi-kafka-source";
            
            var config = new ConsumerConfig
            {
                GroupId = groupId,
                BootstrapServers = configuration.GetValue<string>("bootstrapServers"),
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = false,
                SessionTimeoutMs = 6000,
                HeartbeatIntervalMs = 3000
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
                _client.Subscribe(_topicName);

                // Get saved offsets from state store and seek to them
                await RestoreOffsets(stoppingToken);

                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = _client.Consume(TimeSpan.FromSeconds(5));
                        
                        if (consumeResult == null)
                            continue;

                        long reactivatorStartNs = (DateTimeOffset.UtcNow.Ticks - DateTimeOffset.UnixEpoch.Ticks) * 100;
                        
                        var change = await _eventMapper.MapEventAsync(consumeResult, reactivatorStartNs);
                        
                        await _channel.Writer.WriteAsync(change, stoppingToken);
                        
                        // Save offset to state store
                        await SaveOffset(consumeResult, stoppingToken);
                        
                        _logger.LogDebug("Published change for topic {Topic}, partition {Partition}, offset {Offset}", 
                            consumeResult.Topic, consumeResult.Partition.Value, consumeResult.Offset.Value);
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex, "Error consuming from topic {Topic}: {Message}", _topicName, ex.Error.Reason);
                        await Task.Delay(5000, stoppingToken);
                    }
                    catch (TaskCanceledException)
                    {
                        _logger.LogInformation("Shutting down topic consumer: {Topic}", _topicName);
                        break;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error processing message from topic {Topic}: {Message}", _topicName, ex.Message);
                        await Task.Delay(5000, stoppingToken);
                    }
                }
            }
            finally
            {
                _client.Close();
                _logger.LogInformation("Kafka consumer for topic {Topic} has been stopped", _topicName);
            }
        }

        private async Task RestoreOffsets(CancellationToken cancellationToken)
        {
            try
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
                }

                if (offsetsToSeek.Any())
                {
                    foreach (var offset in offsetsToSeek)
                    {
                        _client.Seek(offset);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error restoring offsets for topic {Topic}", _topicName);
            }
        }

        private async Task SaveOffset(ConsumeResult<string, string> consumeResult, CancellationToken cancellationToken)
        {
            try
            {
                var stateKey = $"{_topicName}-{consumeResult.Partition}";
                var offsetBytes = BitConverter.GetBytes(consumeResult.Offset.Value);
                await _checkpointStore.Put(stateKey, offsetBytes);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving offset for topic {Topic}, partition {Partition}, offset {Offset}", 
                    consumeResult.Topic, consumeResult.Partition.Value, consumeResult.Offset.Value);
            }
        }

        public override void Dispose()
        {
            _client?.Dispose();
            base.Dispose();
        }
    }
}