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
using Drasi.Source.SDK;
using Drasi.Source.SDK.Models;

namespace Proxy.Services
{
    class BootstrapHandler(IEventMapper eventMapper, IConfiguration configuration, ILogger<BootstrapHandler> logger): IBootstrapHandler
    {
        public async IAsyncEnumerable<SourceElement> Bootstrap(BootstrapRequest request, [EnumeratorCancellation]CancellationToken cancellationToken = default)
        {
            var windowSize = configuration.GetValue<long>("bootstrapWindow");
            var groupId = configuration.GetValue<string>("groupId") ?? "drasi-kafka-source";

            if (windowSize <= 0) 
            {
                yield break;
            }

            foreach (var label in request.NodeLabels)
            {
                var consumer = BuildConsumer(groupId);
                
                await foreach (var change in GetTopicData(consumer, label, windowSize, cancellationToken))
                {
                    yield return change;
                }
                
                consumer.Close();
            }
        }

        private IConsumer<string, string> BuildConsumer(string groupId)
        {
            var config = new ConsumerConfig
            {
                GroupId = groupId,
                BootstrapServers = configuration.GetValue<string>("bootstrapServers"),
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
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

            return new ConsumerBuilder<string, string>(config).Build();
        }

        async IAsyncEnumerable<SourceElement> GetTopicData(IConsumer<string, string> consumer, string topic, long windowSize, [EnumeratorCancellation] CancellationToken stoppingToken)
        {
            consumer.Subscribe(topic);
            
            var startTime = DateTimeOffset.UtcNow.AddMinutes(-windowSize);
            var endTime = DateTimeOffset.UtcNow;
            var processedMessages = 0;

            // Wait for partition assignment
            await Task.Delay(2000, stoppingToken);
            
            var assignment = consumer.Assignment;
            if (!assignment.Any())
            {
                logger.LogWarning("No partitions assigned for topic {Topic}", topic);
                consumer.Unsubscribe();
                yield break;
            }

            // For bootstrap, we want to read from a specific time window
            var timestampOffsets = new List<TopicPartitionTimestamp>();
            foreach (var partition in assignment)
            {
                timestampOffsets.Add(new TopicPartitionTimestamp(partition, new Timestamp(startTime)));
            }

            var offsets = consumer.OffsetsForTimes(timestampOffsets, TimeSpan.FromSeconds(10));
            var validOffsets = offsets.Where(o => o.Offset != Offset.Unset).ToList();
            
            if (!validOffsets.Any())
            {
                logger.LogInformation("No messages found in the specified time window for topic {Topic}", topic);
                consumer.Unsubscribe();
                yield break;
            }

            // Seek to calculated offsets
            foreach (var offset in validOffsets)
            {
                consumer.Seek(new TopicPartitionOffset(offset.TopicPartition, offset.Offset));
            }

            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? consumeResult = null;
                
                try
                {
                    consumeResult = consumer.Consume(TimeSpan.FromSeconds(5));
                }
                catch (ConsumeException ex)
                {
                    logger.LogError(ex, "Error consuming message from topic {Topic}", topic);
                    break;
                }
                
                if (consumeResult == null)
                    break;

                // Check if message is within our time window
                if (consumeResult.Message.Timestamp.Type == TimestampType.CreateTime && 
                    consumeResult.Message.Timestamp.UtcDateTime > endTime)
                    break;

                var change = await eventMapper.MapEventAsync(consumeResult);
                yield return change;
                
                processedMessages++;
                
                // Prevent infinite loops
                if (processedMessages > 10000)
                {
                    logger.LogWarning("Processed {Count} messages, stopping bootstrap to prevent infinite loop", processedMessages);
                    break;
                }
            }
            
            logger.LogInformation("Bootstrap completed for topic {Topic}, processed {Count} messages", topic, processedMessages);
            consumer.Unsubscribe();
        }       
    }
}