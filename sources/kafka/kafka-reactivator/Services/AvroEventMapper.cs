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
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Confluent.Kafka;
using Drasi.Source.SDK.Models;
using Microsoft.Extensions.Logging;

namespace Reactivator.Services
{
    /// <summary>
    /// Maps Avro messages from Kafka to Drasi SourceChange objects
    /// </summary>
    public class AvroEventMapper : IEventMapper
    {
        private readonly ILogger<AvroEventMapper> _logger;
        private readonly IConfiguration _configuration;
        private readonly Dictionary<string, string> _schemaCache = new();
        private readonly string _schemaRegistryUrl;

        public string Format => "avro";

        public AvroEventMapper(IConfiguration configuration, ILogger<AvroEventMapper> logger = null)
        {
            _configuration = configuration;
            _logger = logger;
            _schemaRegistryUrl = configuration.GetValue<string>("schemaRegistryUrl", "http://localhost:8081");
        }

        public async Task<SourceChange> MapEventAsync(ConsumeResult<string, string> consumeResult, long reactivatorStartNs)
        {
            try
            {
                // Generate element ID from key or fallback to topic-partition-offset
                var elementId = consumeResult.Message.Key ?? $"{consumeResult.Topic}-{consumeResult.Partition.Value}-{consumeResult.Offset.Value}";
                
                // For Avro messages, we need to:
                // 1. Check if the message is in Confluent Schema Registry format (magic byte + schema ID)
                // 2. If so, get the schema from the registry and deserialize
                // 3. If not, try to deserialize using a local schema if available
                
                JsonObject jsonData;
                
                if (consumeResult.Message.Value.StartsWith("\0")) // Confluent Schema Registry format
                {
                    jsonData = await DeserializeConfluentAvro(consumeResult.Message.Value, consumeResult.Topic);
                }
                else
                {
                    // For this example, we'll just create a simple object with the raw message
                    // In a real implementation, you would use an Avro library to deserialize
                    _logger?.LogWarning("Received non-Confluent Schema Registry Avro message for topic {Topic}, partition {Partition}, offset {Offset}",
                        consumeResult.Topic, consumeResult.Partition.Value, consumeResult.Offset.Value);
                    
                    jsonData = new JsonObject
                    {
                        ["raw_message"] = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(consumeResult.Message.Value)),
                        ["format"] = "avro",
                        ["parse_error"] = "Non-Confluent Schema Registry format not supported"
                    };
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
                _logger?.LogError(ex, "Error mapping Avro event for topic {Topic}, partition {Partition}, offset {Offset}", 
                    consumeResult.Topic, consumeResult.Partition.Value, consumeResult.Offset.Value);
                
                // Create a fallback object with error information
                var jsonData = new JsonObject
                {
                    ["raw_message"] = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(consumeResult.Message.Value)),
                    ["format"] = "avro",
                    ["parse_error"] = ex.Message
                };
                
                var data = new SourceElement(
                    $"{consumeResult.Topic}-{consumeResult.Partition.Value}-{consumeResult.Offset.Value}", 
                    [consumeResult.Topic], 
                    jsonData);
                
                return Task.FromResult(new SourceChange(
                    ChangeOp.INSERT, 
                    data, 
                    reactivatorStartNs, 
                    reactivatorStartNs, 
                    consumeResult.Offset.Value, 
                    consumeResult.Partition.Value.ToString()));
            }
        }

        /// <summary>
        /// Deserializes a message in Confluent Schema Registry format
        /// </summary>
        private async Task<JsonObject> DeserializeConfluentAvro(string message, string topic)
        {
            // Note: This is a simplified implementation. In a real application, you would:
            // 1. Extract the schema ID from the message
            // 2. Fetch the schema from the Schema Registry if not cached
            // 3. Use the schema to deserialize the Avro data
            
            // For this example, we'll just create a placeholder object
            // In a real implementation, you would use Confluent.SchemaRegistry and Confluent.Kafka.Avro libraries
            
            return new JsonObject
            {
                ["format"] = "avro",
                ["topic"] = topic,
                ["schema_registry"] = _schemaRegistryUrl,
                ["message"] = "Avro deserialization not fully implemented in this example"
            };
        }
    }
}