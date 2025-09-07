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

using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;

namespace Reactivator.Configuration
{
    /// <summary>
    /// Provides strongly-typed access to Kafka configuration settings
    /// </summary>
    public class KafkaConfiguration
    {
        private readonly IConfiguration _configuration;

        public KafkaConfiguration(IConfiguration configuration)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        /// <summary>
        /// Gets the bootstrap servers configuration
        /// </summary>
        public string BootstrapServers => _configuration.GetValue<string>("bootstrapServers") ?? 
            throw new InvalidOperationException("bootstrapServers configuration is required");

        /// <summary>
        /// Gets the security protocol to use
        /// </summary>
        public string SecurityProtocol => _configuration.GetValue<string>("securityProtocol", "PLAINTEXT");

        /// <summary>
        /// Gets the SASL mechanism to use
        /// </summary>
        public string SaslMechanism => _configuration.GetValue<string>("saslMechanism", "PLAIN");

        /// <summary>
        /// Gets the SASL username
        /// </summary>
        public string SaslUsername => _configuration.GetValue<string>("saslUsername", "");

        /// <summary>
        /// Gets the SASL password
        /// </summary>
        public string SaslPassword => _configuration.GetValue<string>("saslPassword", "");

        /// <summary>
        /// Gets the SSL CA certificate location
        /// </summary>
        public string SslCaLocation => _configuration.GetValue<string>("sslCaLocation", "");

        /// <summary>
        /// Gets the SSL certificate location
        /// </summary>
        public string SslCertificateLocation => _configuration.GetValue<string>("sslCertificateLocation", "");

        /// <summary>
        /// Gets the SSL key location
        /// </summary>
        public string SslKeyLocation => _configuration.GetValue<string>("sslKeyLocation", "");

        /// <summary>
        /// Gets the SSL key password
        /// </summary>
        public string SslKeyPassword => _configuration.GetValue<string>("sslKeyPassword", "");

        /// <summary>
        /// Gets the consumer group ID
        /// </summary>
        public string GroupId => _configuration.GetValue<string>("groupId", "drasi-kafka-consumer");

        /// <summary>
        /// Gets the auto offset reset behavior
        /// </summary>
        public string AutoOffsetReset => _configuration.GetValue<string>("autoOffsetReset", "earliest");

        /// <summary>
        /// Gets the session timeout in milliseconds
        /// </summary>
        public int SessionTimeoutMs => _configuration.GetValue<int>("sessionTimeoutMs", 10000);

        /// <summary>
        /// Gets the heartbeat interval in milliseconds
        /// </summary>
        public int HeartbeatIntervalMs => _configuration.GetValue<int>("heartbeatIntervalMs", 3000);

        /// <summary>
        /// Gets the maximum poll interval in milliseconds
        /// </summary>
        public int MaxPollIntervalMs => _configuration.GetValue<int>("maxPollIntervalMs", 300000);

        /// <summary>
        /// Gets the fetch min bytes
        /// </summary>
        public int FetchMinBytes => _configuration.GetValue<int>("fetchMinBytes", 1);

        /// <summary>
        /// Gets the fetch max bytes
        /// </summary>
        public int FetchMaxBytes => _configuration.GetValue<int>("fetchMaxBytes", 52428800);

        /// <summary>
        /// Gets the fetch max wait time in milliseconds
        /// </summary>
        public int FetchMaxWaitMs => _configuration.GetValue<int>("fetchMaxWaitMs", 500);

        /// <summary>
        /// Gets the maximum number of messages to process in a single batch
        /// </summary>
        public int MaxBatchSize => _configuration.GetValue<int>("maxBatchSize", 100);

        /// <summary>
        /// Gets the number of consumer threads per topic
        /// </summary>
        public int ConsumerThreadsPerTopic => _configuration.GetValue<int>("consumerThreadsPerTopic", 1);

        /// <summary>
        /// Gets the message format (json, avro, plaintext)
        /// </summary>
        public string MessageFormat => _configuration.GetValue<string>("messageFormat", "json");

        /// <summary>
        /// Gets the retry policy configuration
        /// </summary>
        public RetryPolicyConfiguration RetryPolicy => new RetryPolicyConfiguration(_configuration.GetSection("retryPolicy"));

        /// <summary>
        /// Gets the dead letter topic name
        /// </summary>
        public string DeadLetterTopic => _configuration.GetValue<string>("deadLetterTopic", "");

        /// <summary>
        /// Gets whether to enable auto commit
        /// </summary>
        public bool EnableAutoCommit => _configuration.GetValue<bool>("enableAutoCommit", false);

        /// <summary>
        /// Gets whether to enable auto offset store
        /// </summary>
        public bool EnableAutoOffsetStore => _configuration.GetValue<bool>("enableAutoOffsetStore", false);

        /// <summary>
        /// Gets the client ID prefix
        /// </summary>
        public string ClientIdPrefix => _configuration.GetValue<string>("clientIdPrefix", "drasi-kafka-client");

        /// <summary>
        /// Gets all Kafka configuration as a dictionary
        /// </summary>
        public Dictionary<string, string> GetConsumerConfig()
        {
            var config = new Dictionary<string, string>
            {
                { "bootstrap.servers", BootstrapServers },
                { "group.id", GroupId },
                { "auto.offset.reset", AutoOffsetReset },
                { "enable.auto.commit", EnableAutoCommit.ToString().ToLower() },
                { "enable.auto.offset.store", EnableAutoOffsetStore.ToString().ToLower() },
                { "session.timeout.ms", SessionTimeoutMs.ToString() },
                { "heartbeat.interval.ms", HeartbeatIntervalMs.ToString() },
                { "max.poll.interval.ms", MaxPollIntervalMs.ToString() },
                { "fetch.min.bytes", FetchMinBytes.ToString() },
                { "fetch.max.bytes", FetchMaxBytes.ToString() },
                { "fetch.max.wait.ms", FetchMaxWaitMs.ToString() }
            };

            // Add security configuration if needed
            if (!string.IsNullOrEmpty(SecurityProtocol))
            {
                config["security.protocol"] = SecurityProtocol;
            }

            if (!string.IsNullOrEmpty(SaslMechanism))
            {
                config["sasl.mechanism"] = SaslMechanism;
            }

            if (!string.IsNullOrEmpty(SaslUsername) && !string.IsNullOrEmpty(SaslPassword))
            {
                config["sasl.username"] = SaslUsername;
                config["sasl.password"] = SaslPassword;
            }

            // Add SSL configuration if needed
            if (!string.IsNullOrEmpty(SslCaLocation))
            {
                config["ssl.ca.location"] = SslCaLocation;
            }

            if (!string.IsNullOrEmpty(SslCertificateLocation))
            {
                config["ssl.certificate.location"] = SslCertificateLocation;
            }

            if (!string.IsNullOrEmpty(SslKeyLocation))
            {
                config["ssl.key.location"] = SslKeyLocation;
            }

            if (!string.IsNullOrEmpty(SslKeyPassword))
            {
                config["ssl.key.password"] = SslKeyPassword;
            }

            return config;
        }
    }

    /// <summary>
    /// Configuration for retry policy
    /// </summary>
    public class RetryPolicyConfiguration
    {
        private readonly IConfiguration _configuration;

        public RetryPolicyConfiguration(IConfiguration configuration)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        /// <summary>
        /// Gets the maximum number of retry attempts
        /// </summary>
        public int MaxRetryAttempts => _configuration.GetValue<int>("maxRetryAttempts", 5);

        /// <summary>
        /// Gets the initial retry delay in milliseconds
        /// </summary>
        public int InitialRetryDelayMs => _configuration.GetValue<int>("initialRetryDelayMs", 1000);

        /// <summary>
        /// Gets the maximum retry delay in milliseconds
        /// </summary>
        public int MaxRetryDelayMs => _configuration.GetValue<int>("maxRetryDelayMs", 30000);

        /// <summary>
        /// Gets the retry backoff multiplier
        /// </summary>
        public double BackoffMultiplier => _configuration.GetValue<double>("backoffMultiplier", 2.0);

        /// <summary>
        /// Gets the circuit breaker failure threshold
        /// </summary>
        public int CircuitBreakerThreshold => _configuration.GetValue<int>("circuitBreakerThreshold", 5);

        /// <summary>
        /// Gets the circuit breaker reset timeout in milliseconds
        /// </summary>
        public int CircuitBreakerResetTimeoutMs => _configuration.GetValue<int>("circuitBreakerResetTimeoutMs", 60000);
    }
}