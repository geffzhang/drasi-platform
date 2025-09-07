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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Reactivator.Services
{
    /// <summary>
    /// Collects and exposes metrics for Kafka consumers
    /// </summary>
    public class KafkaMetrics
    {
        private static readonly Lazy<KafkaMetrics> _instance = new Lazy<KafkaMetrics>(() => new KafkaMetrics());
        public static KafkaMetrics Instance => _instance.Value;

        // Metrics storage
        private readonly ConcurrentDictionary<string, TopicMetrics> _topicMetrics = new();
        
        // Periodic metrics reporter
        private Timer? _reportingTimer;
        private ILogger? _logger;

        private KafkaMetrics()
        {
        }

        /// <summary>
        /// Initialize metrics reporting
        /// </summary>
        public void Initialize(ILogger logger, int reportingIntervalMs = 60000)
        {
            _logger = logger;
            _reportingTimer = new Timer(ReportMetrics, null, reportingIntervalMs, reportingIntervalMs);
        }

        /// <summary>
        /// Record message processing time
        /// </summary>
        public void RecordProcessingTime(string topic, TimeSpan processingTime)
        {
            var metrics = _topicMetrics.GetOrAdd(topic, _ => new TopicMetrics(topic));
            metrics.RecordProcessingTime(processingTime);
        }

        /// <summary>
        /// Record message lag (difference between message timestamp and processing time)
        /// </summary>
        public void RecordMessageLag(string topic, TimeSpan lag)
        {
            var metrics = _topicMetrics.GetOrAdd(topic, _ => new TopicMetrics(topic));
            metrics.RecordMessageLag(lag);
        }

        /// <summary>
        /// Record error occurrence
        /// </summary>
        public void RecordError(string topic)
        {
            var metrics = _topicMetrics.GetOrAdd(topic, _ => new TopicMetrics(topic));
            metrics.RecordError();
        }

        /// <summary>
        /// Update statistics from Kafka client
        /// </summary>
        public void UpdateStatistics(string topic, string statisticsJson)
        {
            try
            {
                var metrics = _topicMetrics.GetOrAdd(topic, _ => new TopicMetrics(topic));
                var stats = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(statisticsJson);
                
                if (stats != null)
                {
                    metrics.UpdateKafkaStatistics(stats);
                }
            }
            catch (Exception)
            {
                // Ignore parsing errors
            }
        }

        /// <summary>
        /// Report metrics periodically
        /// </summary>
        private void ReportMetrics(object? state)
        {
            if (_logger == null) return;

            foreach (var metrics in _topicMetrics.Values)
            {
                _logger.LogInformation(
                    "Kafka metrics for topic {Topic}: Messages={MessageCount}, " +
                    "Avg Processing={AvgProcessingMs}ms, Max Processing={MaxProcessingMs}ms, " +
                    "Avg Lag={AvgLagMs}ms, Max Lag={MaxLagMs}ms, " +
                    "Errors={ErrorCount}, Bytes={BytesConsumed}",
                    metrics.Topic,
                    metrics.MessageCount,
                    metrics.AverageProcessingTimeMs,
                    metrics.MaxProcessingTimeMs,
                    metrics.AverageLagMs,
                    metrics.MaxLagMs,
                    metrics.ErrorCount,
                    metrics.BytesConsumed);
                
                // Reset metrics after reporting
                metrics.Reset();
            }
        }

        /// <summary>
        /// Metrics for a single topic
        /// </summary>
        private class TopicMetrics
        {
            public string Topic { get; }
            
            // Message processing metrics
            public long MessageCount { get; private set; }
            public double TotalProcessingTimeMs { get; private set; }
            public double MaxProcessingTimeMs { get; private set; }
            public double AverageProcessingTimeMs => MessageCount > 0 ? TotalProcessingTimeMs / MessageCount : 0;
            
            // Message lag metrics
            public double TotalLagMs { get; private set; }
            public double MaxLagMs { get; private set; }
            public double AverageLagMs => MessageCount > 0 ? TotalLagMs / MessageCount : 0;
            
            // Error metrics
            public int ErrorCount { get; private set; }
            
            // Kafka client metrics
            public long BytesConsumed { get; private set; }
            
            public TopicMetrics(string topic)
            {
                Topic = topic;
                Reset();
            }

            public void RecordProcessingTime(TimeSpan processingTime)
            {
                var ms = processingTime.TotalMilliseconds;
                MessageCount++;
                TotalProcessingTimeMs += ms;
                MaxProcessingTimeMs = Math.Max(MaxProcessingTimeMs, ms);
            }

            public void RecordMessageLag(TimeSpan lag)
            {
                var ms = lag.TotalMilliseconds;
                TotalLagMs += ms;
                MaxLagMs = Math.Max(MaxLagMs, ms);
            }

            public void RecordError()
            {
                ErrorCount++;
            }

            public void UpdateKafkaStatistics(Dictionary<string, JsonElement> stats)
            {
                // Extract relevant metrics from Kafka statistics
                if (stats.TryGetValue("size", out var size) && size.ValueKind == JsonValueKind.Number)
                {
                    BytesConsumed = size.GetInt64();
                }
            }

            public void Reset()
            {
                MessageCount = 0;
                TotalProcessingTimeMs = 0;
                MaxProcessingTimeMs = 0;
                TotalLagMs = 0;
                MaxLagMs = 0;
                ErrorCount = 0;
                // Don't reset BytesConsumed as it's cumulative from Kafka stats
            }
        }
    }
}