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
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Confluent.Kafka;
using Drasi.Source.SDK.Models;
using Microsoft.Extensions.Logging;

namespace Reactivator.Services 
{
    /// <summary>
    /// Maps JSON messages from Kafka to Drasi SourceChange objects
    /// </summary>
    public class JsonEventMapper : IEventMapper
    {
        private readonly ILogger<JsonEventMapper> _logger;
        private readonly JsonSerializerOptions _jsonOptions;

        public string Format => "json";

        public JsonEventMapper(ILogger<JsonEventMapper> logger = null)
        {
            _logger = logger;
            _jsonOptions = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
                AllowTrailingCommas = true,
                ReadCommentHandling = JsonCommentHandling.Skip
            };
        }

        public Task<SourceChange> MapEventAsync(ConsumeResult<string, string> consumeResult, long reactivatorStartNs)
        {
            try
            {
                // Generate element ID from key or fallback to topic-partition-offset
                var elementId = consumeResult.Message.Key ?? $"{consumeResult.Topic}-{consumeResult.Partition.Value}-{consumeResult.Offset.Value}";
                
                // Parse JSON message
                JsonObject jsonData;
                try
                {
                    var jsonNode = JsonNode.Parse(consumeResult.Message.Value, _jsonOptions);
                    jsonData = jsonNode?.AsObject();
                    
                    if (jsonData == null)
                    {
                        throw new JsonException("Failed to parse message as JSON object");
                    }
                }
                catch (JsonException ex)
                {
                    _logger?.LogWarning(ex, "Failed to parse message as JSON for topic {Topic}, partition {Partition}, offset {Offset}. Message: {Message}", 
                        consumeResult.Topic, consumeResult.Partition.Value, consumeResult.Offset.Value, 
                        consumeResult.Message.Value?.Length > 100 ? consumeResult.Message.Value.Substring(0, 100) + "..." : consumeResult.Message.Value);
                    
                    // Create a simple object with the raw message as a fallback
                    jsonData = new JsonObject
                    {
                        ["raw_message"] = consumeResult.Message.Value,
                        ["parse_error"] = ex.Message
                    };
                }

                // Create source element with the parsed data
                var data = new SourceElement(elementId, [consumeResult.Topic], jsonData);

                // Convert Kafka timestamp to nanoseconds - if timestamp is available, use it, otherwise use current time
                var eventTimeNs = consumeResult.Message.Timestamp.Type == TimestampType.CreateTime
                    ? consumeResult.Message.Timestamp.UnixTimestampMs * 1000000
                    : reactivatorStartNs;

                // Determine operation type (default to INSERT)
                var operation = DetermineOperation(jsonData);

                return Task.FromResult(new SourceChange(
                    operation, 
                    data, 
                    eventTimeNs, 
                    reactivatorStartNs, 
                    consumeResult.Offset.Value, 
                    consumeResult.Partition.Value.ToString()));
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error mapping JSON event for topic {Topic}, partition {Partition}, offset {Offset}", 
                    consumeResult.Topic, consumeResult.Partition.Value, consumeResult.Offset.Value);
                throw;
            }
        }

        /// <summary>
        /// Determines the operation type from the JSON data
        /// </summary>
        private ChangeOp DetermineOperation(JsonObject jsonData)
        {
            // Check if the JSON contains an operation field
            if (jsonData.TryGetPropertyValue("operation", out var opNode) && opNode is JsonValue opValue)
            {
                var opString = opValue.ToString().ToUpperInvariant();
                
                return opString switch
                {
                    "DELETE" => ChangeOp.DELETE,
                    "UPDATE" => ChangeOp.UPDATE,
                    "INSERT" => ChangeOp.INSERT,
                    _ => ChangeOp.INSERT // Default to INSERT for unknown operations
                };
            }
            
            // Default to INSERT if no operation field is found
            return ChangeOp.INSERT;
        }
    }
}