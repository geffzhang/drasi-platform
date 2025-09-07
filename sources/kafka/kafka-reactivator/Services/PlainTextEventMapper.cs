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
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Confluent.Kafka;
using Drasi.Source.SDK.Models;
using Microsoft.Extensions.Logging;

namespace Reactivator.Services
{
    /// <summary>
    /// Maps plain text messages from Kafka to Drasi SourceChange objects
    /// </summary>
    public class PlainTextEventMapper : IEventMapper
    {
        private readonly ILogger<PlainTextEventMapper> _logger;
        private readonly IConfiguration _configuration;
        private readonly string _textFieldName;

        public string Format => "text";

        public PlainTextEventMapper(IConfiguration configuration, ILogger<PlainTextEventMapper> logger = null)
        {
            _configuration = configuration;
            _logger = logger;
            _textFieldName = configuration.GetValue<string>("textFieldName", "content");
        }

        public Task<SourceChange> MapEventAsync(ConsumeResult<string, string> consumeResult, long reactivatorStartNs)
        {
            try
            {
                // Generate element ID from key or fallback to topic-partition-offset
                var elementId = consumeResult.Message.Key ?? $"{consumeResult.Topic}-{consumeResult.Partition.Value}-{consumeResult.Offset.Value}";
                
                // Create a JSON object with the text content
                var jsonData = new JsonObject
                {
                    [_textFieldName] = consumeResult.Message.Value,
                    ["topic"] = consumeResult.Topic,
                    ["partition"] = consumeResult.Partition.Value,
                    ["offset"] = consumeResult.Offset.Value
                };

                // Add message headers if available
                if (consumeResult.Message.Headers != null && consumeResult.Message.Headers.Count > 0)
                {
                    var headers = new JsonObject();
                    foreach (var header in consumeResult.Message.Headers)
                    {
                        if (header.Value != null)
                        {
                            headers[header.Key] = System.Text.Encoding.UTF8.GetString(header.Value);
                        }
                    }
                    jsonData["headers"] = headers;
                }

                // Add timestamp if available
                if (consumeResult.Message.Timestamp.Type == TimestampType.CreateTime)
                {
                    jsonData["timestamp"] = DateTimeOffset.FromUnixTimeMilliseconds(consumeResult.Message.Timestamp.UnixTimestampMs).ToString("o");
                }

                // Create source element with the parsed data
                var data = new SourceElement(elementId, [consumeResult.Topic], jsonData);

                // Convert Kafka timestamp to nanoseconds - if timestamp is available, use it, otherwise use current time
                var eventTimeNs = consumeResult.Message.Timestamp.Type == TimestampType.CreateTime
                    ? consumeResult.Message.Timestamp.UnixTimestampMs * 1000000
                    : reactivatorStartNs;

                return Task.FromResult(new SourceChange(
                    ChangeOp.INSERT, 
                    data, 
                    eventTimeNs, 
                    reactivatorStartNs, 
                    consumeResult.Offset.Value, 
                    consumeResult.Partition.Value.ToString()));
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error mapping text event for topic {Topic}, partition {Partition}, offset {Offset}", 
                    consumeResult.Topic, consumeResult.Partition.Value, consumeResult.Offset.Value);
                throw;
            }
        }
    }
}