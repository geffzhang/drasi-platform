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

using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Reactivator.Configuration;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Reactivator.Services
{
    /// <summary>
    /// Handles messages that could not be processed successfully after multiple retries
    /// </summary>
    public class DeadLetterHandler
    {
        private readonly KafkaConfiguration _kafkaConfig;
        private readonly ILogger<DeadLetterHandler> _logger;
        private IProducer<string, string>? _producer;

        public DeadLetterHandler(KafkaConfiguration kafkaConfig, ILogger<DeadLetterHandler> logger)
        {
            _kafkaConfig = kafkaConfig ?? throw new ArgumentNullException(nameof(kafkaConfig));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Sends a failed message to the dead letter topic
        /// </summary>
        /// <param name="message">The original Kafka message</param>
        /// <param name="error">The error that occurred</param>
        /// <returns>A task representing the asynchronous operation</returns>
        public async Task SendToDeadLetterTopicAsync(ConsumeResult<string, string> message, Exception error)
        {
            if (string.IsNullOrEmpty(_kafkaConfig.DeadLetterTopic))
            {
                _logger.LogWarning("Dead letter topic not configured. Failed message will be lost.");
                return;
            }

            try
            {
                // Initialize producer if needed
                _producer ??= CreateProducer();

                // Create dead letter message with metadata
                var deadLetterMessage = new DeadLetterMessage
                {
                    OriginalTopic = message.Topic,
                    OriginalPartition = message.Partition.Value,
                    OriginalOffset = message.Offset.Value,
                    OriginalKey = message.Key,
                    OriginalValue = message.Value,
                    ErrorMessage = error.Message,
                    ErrorType = error.GetType().Name,
                    ErrorStackTrace = error.StackTrace,
                    Timestamp = DateTimeOffset.UtcNow
                };

                // Serialize the dead letter message
                var deadLetterValue = JsonSerializer.Serialize(deadLetterMessage);

                // Send to dead letter topic
                var deliveryResult = await _producer.ProduceAsync(_kafkaConfig.DeadLetterTopic, new Message<string, string>
                {
                    Key = message.Key,
                    Value = deadLetterValue,
                    Headers = message.Headers
                });

                _logger.LogInformation(
                    "Message sent to dead letter topic {Topic} at partition {Partition}, offset {Offset}",
                    deliveryResult.Topic, deliveryResult.Partition, deliveryResult.Offset);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to send message to dead letter topic {Topic}", _kafkaConfig.DeadLetterTopic);
            }
        }

        /// <summary>
        /// Creates a Kafka producer for the dead letter topic
        /// </summary>
        private IProducer<string, string> CreateProducer()
        {
            var config = new ProducerConfig(_kafkaConfig.GetConsumerConfig())
            {
                ClientId = $"{_kafkaConfig.ClientIdPrefix}-dead-letter-producer",
                Acks = Acks.All,
                EnableIdempotence = true,
                MaxInFlight = 5,
                MessageSendMaxRetries = 3,
                RetryBackoffMs = 1000
            };

            return new ProducerBuilder<string, string>(config).Build();
        }

        /// <summary>
        /// Disposes the producer if it exists
        /// </summary>
        public void Dispose()
        {
            _producer?.Dispose();
            _producer = null;
        }
    }

    /// <summary>
    /// Represents a message that could not be processed and is sent to the dead letter topic
    /// </summary>
    public class DeadLetterMessage
    {
        /// <summary>
        /// The original topic the message was consumed from
        /// </summary>
        public string OriginalTopic { get; set; } = string.Empty;

        /// <summary>
        /// The original partition the message was consumed from
        /// </summary>
        public int OriginalPartition { get; set; }

        /// <summary>
        /// The original offset of the message
        /// </summary>
        public long OriginalOffset { get; set; }

        /// <summary>
        /// The original key of the message
        /// </summary>
        public string? OriginalKey { get; set; }

        /// <summary>
        /// The original value of the message
        /// </summary>
        public string? OriginalValue { get; set; }

        /// <summary>
        /// The error message that occurred
        /// </summary>
        public string ErrorMessage { get; set; } = string.Empty;

        /// <summary>
        /// The type of error that occurred
        /// </summary>
        public string ErrorType { get; set; } = string.Empty;

        /// <summary>
        /// The stack trace of the error
        /// </summary>
        public string? ErrorStackTrace { get; set; }

        /// <summary>
        /// The timestamp when the message was sent to the dead letter topic
        /// </summary>
        public DateTimeOffset Timestamp { get; set; }
    }
}