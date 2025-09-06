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
    class KafkaConsumer : BackgroundService
    {
        private readonly Channel<SourceChange> _channel;
        private readonly IStateStore _checkpointStore;
        private readonly IEventMapper _eventMapper;
        private readonly IConfiguration _configuration;
        private readonly IConsumer<string, string> _client;
        private readonly string _topicName;
        private readonly ILogger _logger;

        public KafkaConsumer(Channel<SourceChange> channel, IStateStore stateStore, IEventMapper eventMapper, IConfiguration configuration, ILogger logger, string topicName)
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
            var consumerGroup = configuration.GetValue<string>("consumerGroup") ?? "drasi-reactivator";
            
            var configDict = new Dictionary<string, string>
            {
                { "group.id", consumerGroup },
                { "bootstrap.servers", configuration.GetValue<string>("bootstrapServers") ?? "localhost:9092" },
                { "auto.offset.reset", "latest" },
                { "enable.auto.commit", "false" },
                { "enable.partition.eof", "false" }
            };

            // Add authentication if configured
            var securityProtocol = configuration.GetValue<string>("securityProtocol");
            if (!string.IsNullOrEmpty(securityProtocol))
            {
                configDict["security.protocol"] = securityProtocol;
                
                var saslMechanism = configuration.GetValue<string>("saslMechanism");
                if (!string.IsNullOrEmpty(saslMechanism))
                {
                    configDict["sasl.mechanism"] = saslMechanism;
                    configDict["sasl.username"] = configuration.GetValue<string>("saslUsername") ?? "";
                    configDict["sasl.password"] = configuration.GetValue<string>("saslPassword") ?? "";
                }
            }

            var config = new ConsumerConfig(configDict);
            return new ConsumerBuilder<string, string>(config).Build();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation($"Starting Kafka consumer for topic {_topicName}");
            
            // Subscribe to the topic
            _client.Subscribe(_topicName);
            
            try
            {
                await ConsumeMessages(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation($"Kafka consumer for topic {_topicName} was cancelled");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error in Kafka consumer for topic {_topicName}: {ex.Message}");
            }
            finally
            {
                _client.Close();
                _client.Dispose();
            }
        }

        private async Task ConsumeMessages(CancellationToken stoppingToken)
        {
            // Get last committed offset for each partition
            var assignment = new List<TopicPartition>();
            
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _client.Consume(TimeSpan.FromSeconds(1));
                    if (consumeResult == null)
                        continue;

                    // Check if we need to restore offset for this partition
                    var partitionKey = $"{_topicName}-{consumeResult.Partition.Value}";
                    var lastOffsetBytes = await _checkpointStore.Get(partitionKey);
                    
                    if (lastOffsetBytes != null)
                    {
                        var lastOffset = BitConverter.ToInt64(lastOffsetBytes);
                        if (consumeResult.Offset.Value <= lastOffset)
                        {
                            // Skip this message as it's already been processed
                            continue;
                        }
                    }

                    long reactivatorStartNs = (DateTimeOffset.UtcNow.Ticks - DateTimeOffset.UnixEpoch.Ticks) * 100;
                    var change = await _eventMapper.MapEventAsync(consumeResult, reactivatorStartNs);
                    
                    await _channel.Writer.WriteAsync(change, stoppingToken);
                    
                    // Store the offset as checkpoint
                    await _checkpointStore.Put(partitionKey, BitConverter.GetBytes(consumeResult.Offset.Value));
                    
                    _logger.LogInformation($"Published change for topic {_topicName} partition {consumeResult.Partition.Value} at offset {consumeResult.Offset.Value}");
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError($"Consume error for topic {_topicName}: {ex.Error.Reason}");
                    await Task.Delay(5000, stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Error consuming from topic {_topicName}: {ex.Message}");
                    await Task.Delay(5000, stoppingToken);
                }
            }
        }
    }
}