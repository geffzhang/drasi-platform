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

using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Drasi.Source.SDK;
using Drasi.Source.SDK.Models;

namespace Proxy.Services
{
    class BootstrapHandler(IEventMapper eventMapper, IConfiguration configuration, ILogger<BootstrapHandler> logger): IBootstrapHandler
    {
        public async IAsyncEnumerable<SourceElement> Bootstrap(BootstrapRequest request, [EnumeratorCancellation]CancellationToken cancellationToken = default)
        {
            var windowSize = configuration.GetValue<long>("bootstrapWindow");
            var consumerGroup = configuration.GetValue<string>("consumerGroup") ?? "drasi-bootstrap";

            if (windowSize <= 0) 
            {
                yield break;
            }

            foreach (var topic in request.NodeLabels)
            {
                var consumer = BuildConsumer(topic, consumerGroup);
                var partitions = GetTopicPartitions(consumer, topic);

                foreach (var partition in partitions)
                {
                    await foreach (var change in GetPartitionData(consumer, partition, windowSize, cancellationToken))
                    {
                        yield return change;
                    }
                }
                
                consumer.Close();
                consumer.Dispose();
            }
        }

        private IConsumer<string, string> BuildConsumer(string topic, string consumerGroup)
        {
            var configDict = new Dictionary<string, string>
            {
                { "group.id", consumerGroup },
                { "bootstrap.servers", configuration.GetValue<string>("bootstrapServers") ?? "localhost:9092" },
                { "auto.offset.reset", "earliest" },
                { "enable.auto.commit", "false" }
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

        private List<TopicPartition> GetTopicPartitions(IConsumer<string, string> consumer, string topic)
        {
            try
            {
                // Use AdminClient to get topic metadata
                var adminConfig = new AdminClientConfig
                {
                    BootstrapServers = configuration.GetValue<string>("bootstrapServers") ?? "localhost:9092"
                };
                
                using var adminClient = new AdminClientBuilder(adminConfig).Build();
                var metadata = adminClient.GetMetadata(topic, TimeSpan.FromSeconds(10));
                var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic == topic);
                
                if (topicMetadata == null)
                {
                    logger.LogWarning($"Topic {topic} not found");
                    return new List<TopicPartition>();
                }

                return topicMetadata.Partitions.Select(p => new TopicPartition(topic, p.PartitionId)).ToList();
            }
            catch (Exception ex)
            {
                logger.LogError($"Error getting metadata for topic {topic}: {ex.Message}");
                return new List<TopicPartition>();
            }
        }

        async IAsyncEnumerable<SourceElement> GetPartitionData(IConsumer<string, string> consumer, TopicPartition partition, long windowSize, [EnumeratorCancellation] CancellationToken stoppingToken)
        {
            var startTime = DateTimeOffset.UtcNow.AddMinutes(-windowSize);
            var endTime = DateTimeOffset.UtcNow;
            
            // Get watermark offsets to determine the range
            WatermarkOffsets watermark;
            List<TopicPartitionOffset> offsetsByTime;
            
            try
            {
                watermark = consumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(10));
                
                if (watermark.High <= watermark.Low)
                {
                    yield break;
                }

                // Try to get offset by timestamp for the start time
                var timestampToSearch = new Timestamp(startTime);
                var offsetsToSearch = new List<TopicPartitionTimestamp> { new(partition, timestampToSearch) };
                offsetsByTime = consumer.OffsetsForTimes(offsetsToSearch, TimeSpan.FromSeconds(10));
            }
            catch (Exception ex)
            {
                logger.LogError($"Error querying partition metadata {partition.Topic}-{partition.Partition}: {ex.Message}");
                yield break;
            }
            
            var startOffset = offsetsByTime.FirstOrDefault()?.Offset ?? watermark.Low;
            
            // Assign the partition and seek to the start offset
            consumer.Assign(partition);
            consumer.Seek(new TopicPartitionOffset(partition, startOffset));

            logger.LogInformation($"Bootstrap reading from topic {partition.Topic} partition {partition.Partition} starting at offset {startOffset}");

            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? consumeResult = null;
                
                try
                {
                    consumeResult = consumer.Consume(TimeSpan.FromSeconds(1));
                }
                catch (Exception ex)
                {
                    logger.LogError($"Error consuming from partition {partition.Topic}-{partition.Partition}: {ex.Message}");
                    break;
                }
                
                if (consumeResult == null)
                    break;

                // Check if message is within our time window
                if (consumeResult.Message.Timestamp.Type == TimestampType.CreateTime)
                {
                    var messageTime = consumeResult.Message.Timestamp.UtcDateTime;
                    if (messageTime < startTime)
                        continue;
                    if (messageTime > endTime)
                        break;
                }

                SourceElement element;
                try
                {
                    element = await eventMapper.MapEventAsync(consumeResult);
                }
                catch (Exception ex)
                {
                    logger.LogError($"Error mapping message at offset {consumeResult.Offset}: {ex.Message}");
                    continue;
                }
                
                yield return element;
            }
            
            try
            {
                consumer.Unassign();
            }
            catch (Exception ex)
            {
                logger.LogError($"Error unassigning partition {partition.Topic}-{partition.Partition}: {ex.Message}");
            }
        }       
    }
}