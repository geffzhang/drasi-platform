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

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Drasi.Source.SDK;
using Drasi.Source.SDK.Models;

namespace Proxy.Services
{
    /// <summary>
    /// Handles bootstrapping data from Kafka topics
    /// </summary>
    class BootstrapHandler : IBootstrapHandler
    {
        private readonly EventMapperFactory _eventMapperFactory;
        private readonly IConfiguration _configuration;
        private readonly ILogger<BootstrapHandler> _logger;
        private readonly int _batchSize;
        private readonly int _maxParallelTopics;
        private readonly int _maxMessagesPerTopic;
        private readonly int _partitionAssignmentTimeoutMs;
        private readonly string _messageFormat;

        public BootstrapHandler(EventMapperFactory eventMapperFactory, IConfiguration configuration, ILogger<BootstrapHandler> logger)
        {
            _eventMapperFactory = eventMapperFactory;
            _configuration = configuration;
            _logger = logger;
            _messageFormat = configuration.GetValue<string>("messageFormat", "json");
            
            // Get configuration values with defaults
            _batchSize = configuration.GetValue<int>("batchSize", 100);
            _maxParallelTopics = configuration.GetValue<int>("maxParallelTopics", 3);
            _maxMessagesPerTopic = configuration.GetValue<int>("maxMessagesPerTopic", 10000);
            _partitionAssignmentTimeoutMs = configuration.GetValue<int>("partitionAssignmentTimeoutMs", 10000);
        }

        public async IAsyncEnumerable<SourceElement> Bootstrap(BootstrapRequest request, [EnumeratorCancellation]CancellationToken cancellationToken = default)
        {
            var windowSize = _configuration.GetValue<long>("bootstrapWindow");
            var groupId = _configuration.GetValue<string>("groupId") ?? "drasi-kafka-source";

            if (windowSize <= 0) 
            {
                _logger.LogInformation("Bootstrap window is set to {WindowSize}, skipping bootstrap", windowSize);
                yield break;
            }

            // Process topics in parallel with limited concurrency
            var semaphore = new SemaphoreSlim(_maxParallelTopics);
            var tasks = new List<Task<List<SourceElement>>>();
            
            foreach (var topic in request.NodeLabels)
            {
                await semaphore.WaitAsync(cancellationToken);
                
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        return await GetTopicDataBatch(topic, windowSize, cancellationToken);
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }, cancellationToken));
            }
            
            // Process results as they complete
            while (tasks.Count > 0)
            {
                var completedTask = await Task.WhenAny(tasks);
                tasks.Remove(completedTask);
                
                var elements = await completedTask;
                foreach (var element in elements)
                {
                    yield return element;
                }
            }
        }

        /// <summary>
        /// Gets data from a topic in batches
        /// </summary>
        private async Task<List<SourceElement>> GetTopicDataBatch(string topic, long windowSize, CancellationToken cancellationToken)
        {
            var stopwatch = Stopwatch.StartNew();
            var results = new List<SourceElement>();
            var groupId = _configuration.GetValue<string>("groupId") ?? "drasi-kafka-source";
            
            using var consumer = BuildConsumer(groupId);
            
            try
            {
                _logger.LogInformation("Starting bootstrap for topic {Topic} with window size {WindowSize} minutes", 
                    topic, windowSize);
                
                consumer.Subscribe(topic);
                
                var startTime = DateTimeOffset.UtcNow.AddMinutes(-windowSize);
                var endTime = DateTimeOffset.UtcNow;
                
                // Wait for partition assignment with timeout
                var assignmentTimeout = Task.Delay(_partitionAssignmentTimeoutMs, cancellationToken);
                while (!consumer.Assignment.Any())
                {
                    if (await Task.WhenAny(Task.Delay(100, cancellationToken), assignmentTimeout) == assignmentTimeout)
                    {
                        _logger.LogWarning("Timed out waiting for partition assignment for topic {Topic}", topic);
                        return results;
                    }
                }
                
                var assignment = consumer.Assignment;
                _logger.LogInformation("Assigned {PartitionCount} partitions for topic {Topic}", 
                    assignment.Count, topic);
                
                // For bootstrap, we want to read from a specific time window
                var timestampOffsets = assignment.Select(partition => 
                    new TopicPartitionTimestamp(partition, new Timestamp(startTime))).ToList();
                
                var offsets = consumer.OffsetsForTimes(timestampOffsets, TimeSpan.FromSeconds(10));
                var validOffsets = offsets.Where(o => o.Offset != Offset.Unset).ToList();
                
                if (!validOffsets.Any())
                {
                    _logger.LogInformation("No messages found in the specified time window for topic {Topic}", topic);
                    return results;
                }
                
                // Seek to calculated offsets
                foreach (var offset in validOffsets)
                {
                    consumer.Seek(new TopicPartitionOffset(offset.TopicPartition, offset.Offset));
                    _logger.LogDebug("Seeking to offset {Offset} for topic {Topic}, partition {Partition}", 
                        offset.Offset, topic, offset.TopicPartition.Partition.Value);
                }
                
                var processedMessages = 0;
                var batch = new List<ConsumeResult<string, string>>(_batchSize);
                
                while (!cancellationToken.IsCancellationRequested && processedMessages < _maxMessagesPerTopic)
                {
                    // Consume a batch of messages
                    batch.Clear();
                    
                    for (int i = 0; i < _batchSize; i++)
                    {
                        var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(100));
                        if (consumeResult == null)
                            break;
                            
                        // Check if message is within our time window
                        if (consumeResult.Message.Timestamp.Type == TimestampType.CreateTime && 
                            consumeResult.Message.Timestamp.UtcDateTime > endTime)
                            break;
                            
                        batch.Add(consumeResult);
                    }
                    
                    if (batch.Count == 0)
                        break;
                        
                    // Process the batch in parallel
                    var eventMapper = _eventMapperFactory.GetMapper(_messageFormat);
                    var batchResults = await Task.WhenAll(batch.Select(async message => 
                        await eventMapper.MapEventAsync(message)));
                        
                    results.AddRange(batchResults);
                    processedMessages += batch.Count;
                    
                    _logger.LogDebug("Processed batch of {BatchSize} messages for topic {Topic}, total: {Total}", 
                        batch.Count, topic, processedMessages);
                }
                
                stopwatch.Stop();
                _logger.LogInformation("Bootstrap completed for topic {Topic}, processed {Count} messages in {ElapsedMs}ms", 
                    topic, processedMessages, stopwatch.ElapsedMilliseconds);
                    
                return results;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error bootstrapping data from topic {Topic}", topic);
                return results;
            }
            finally
            {
                try
                {
                    consumer.Unsubscribe();
                    consumer.Close();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error closing Kafka consumer for topic {Topic}", topic);
                }
            }
        }

        /// <summary>
        /// Builds a Kafka consumer with the configured settings
        /// </summary>
        private IConsumer<string, string> BuildConsumer(string groupId)
        {
            var config = new ConsumerConfig
            {
                GroupId = groupId,
                BootstrapServers = _configuration.GetValue<string>("bootstrapServers"),
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                // Performance tuning
                FetchMaxBytes = _configuration.GetValue<int>("fetchMaxBytes", 1048576),
                FetchMinBytes = _configuration.GetValue<int>("fetchMinBytes", 1),
                FetchMaxWaitMs = _configuration.GetValue<int>("fetchMaxWaitMs", 500),
                MaxPollIntervalMs = _configuration.GetValue<int>("maxPollIntervalMs", 300000),
                SessionTimeoutMs = _configuration.GetValue<int>("sessionTimeoutMs", 10000),
                // Security settings
                SecurityProtocol = SecurityProtocol.Plaintext
            };

            // Add authentication if provided
            var username = _configuration.GetValue<string>("username");
            var password = _configuration.GetValue<string>("password");
            
            if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password))
            {
                config.SecurityProtocol = SecurityProtocol.SaslPlaintext;
                config.SaslMechanism = SaslMechanism.Plain;
                config.SaslUsername = username;
                config.SaslPassword = password;
            }

            // Add SSL configuration if enabled
            var useSsl = _configuration.GetValue<bool>("useSsl", false);
            if (useSsl)
            {
                config.SecurityProtocol = string.IsNullOrEmpty(username) 
                    ? SecurityProtocol.Ssl 
                    : SecurityProtocol.SaslSsl;
                    
                config.SslCaLocation = _configuration.GetValue<string>("sslCaLocation");
                config.SslCertificateLocation = _configuration.GetValue<string>("sslCertificateLocation");
                config.SslKeyLocation = _configuration.GetValue<string>("sslKeyLocation");
                config.SslKeyPassword = _configuration.GetValue<string>("sslKeyPassword");
            }

            return new ConsumerBuilder<string, string>(config).Build();
        }
    }
}