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
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Reactivator.Configuration;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Reactivator.Services
{
    /// <summary>
    /// Health check for Kafka connectivity
    /// </summary>
    public class KafkaHealthCheck : IHealthCheck
    {
        private readonly KafkaConfiguration _kafkaConfig;
        private readonly ILogger<KafkaHealthCheck> _logger;
        private readonly KafkaMetrics _metrics;

        public KafkaHealthCheck(KafkaConfiguration kafkaConfig, ILogger<KafkaHealthCheck> logger, KafkaMetrics metrics)
        {
            _kafkaConfig = kafkaConfig ?? throw new ArgumentNullException(nameof(kafkaConfig));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        }

        /// <summary>
        /// Checks the health of Kafka connectivity
        /// </summary>
        public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            try
            {
                // Get consumer metrics
                var consumerMetrics = _metrics.GetConsumerMetrics();
                
                // Check if any consumers are active
                if (consumerMetrics.ActiveConsumers == 0)
                {
                    return HealthCheckResult.Degraded("No active Kafka consumers");
                }

                // Check if there are any errors
                if (consumerMetrics.ErrorCount > 0)
                {
                    return HealthCheckResult.Degraded($"Kafka consumers have encountered {consumerMetrics.ErrorCount} errors");
                }

                // Check if there are any circuit breakers open
                if (consumerMetrics.OpenCircuitBreakers > 0)
                {
                    return HealthCheckResult.Degraded($"{consumerMetrics.OpenCircuitBreakers} Kafka circuit breakers are open");
                }

                // Check broker connectivity
                var adminClientConfig = new AdminClientConfig(_kafkaConfig.GetConsumerConfig())
                {
                    ClientId = $"{_kafkaConfig.ClientIdPrefix}-health-check"
                };

                using var adminClient = new AdminClientBuilder(adminClientConfig).Build();
                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));

                // Check if we have brokers
                if (metadata.Brokers.Count == 0)
                {
                    return HealthCheckResult.Unhealthy("No Kafka brokers available");
                }

                // Return healthy with data
                var data = new Dictionary<string, object>
                {
                    { "BrokerCount", metadata.Brokers.Count },
                    { "ActiveConsumers", consumerMetrics.ActiveConsumers },
                    { "MessageProcessed", consumerMetrics.MessagesProcessed },
                    { "AverageProcessingTimeMs", consumerMetrics.AverageProcessingTimeMs },
                    { "LastMessageProcessedAt", consumerMetrics.LastMessageProcessedAt }
                };

                return HealthCheckResult.Healthy("Kafka connection is healthy", data: data);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Kafka health check failed");
                return HealthCheckResult.Unhealthy("Kafka connection check failed", ex);
            }
        }
    }
}