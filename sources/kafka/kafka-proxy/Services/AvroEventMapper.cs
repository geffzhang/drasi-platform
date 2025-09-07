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
using System.IO;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Drasi.Source.SDK.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Proxy.Services 
{
    /// <summary>
    /// Maps Avro messages from Kafka to Drasi SourceElement objects
    /// </summary>
    public class AvroEventMapper : IEventMapper
    {
        private readonly ILogger<AvroEventMapper> _logger;
        private readonly IConfiguration _configuration;
        private readonly Schema _schema;
        private readonly DatumReader<GenericRecord> _datumReader;

        public string Format => "avro";

        public AvroEventMapper(
            ILogger<AvroEventMapper> logger,
            IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
            
            // Try to load schema from configuration
            var schemaJson = _configuration.GetValue<string>("avroSchema");
            if (!string.IsNullOrEmpty(schemaJson))
            {
                try
                {
                    _schema = Schema.Parse(schemaJson);
                    _datumReader = new GenericDatumReader<GenericRecord>(_schema);
                    _logger.LogInformation("Avro schema loaded from configuration");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to parse Avro schema from configuration");
                    throw;
                }
            }
            else
            {
                _logger.LogWarning("No Avro schema provided in configuration. Will attempt to use schema registry or infer schema from data.");
                _schema = null;
                _datumReader = null;
            }
        }

        public Task<SourceElement> MapEventAsync(ConsumeResult<string, string> consumeResult)
        {
            try
            {
                // Generate element ID from key or fallback to topic-partition-offset
                var elementId = consumeResult.Message.Key ?? $"{consumeResult.Topic}-{consumeResult.Partition.Value}-{consumeResult.Offset.Value}";
                
                // Parse Avro message
                JsonObject jsonData;
                
                try
                {
                    // For proxy, we're assuming the message is already in string format
                    // In a real implementation, you might need to handle binary Avro data
                    if (_schema != null)
                    {
                        // If we have a schema, use it to decode
                        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(consumeResult.Message.Value));
                        var decoder = new Avro.IO.BinaryDecoder(stream);
                        var record = _datumReader.Read(null, decoder);
                        
                        // Convert Avro record to JSON
                        jsonData = ConvertAvroRecordToJson(record);
                    }
                    else
                    {
                        // If no schema, try to parse as JSON (assuming it's already been converted)
                        var jsonNode = JsonNode.Parse(consumeResult.Message.Value);
                        jsonData = jsonNode?.AsObject() ?? new JsonObject();
                    }
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(ex, "Failed to parse message as Avro for topic {Topic}, partition {Partition}, offset {Offset}",
                        consumeResult.Topic, consumeResult.Partition.Value, consumeResult.Offset.Value);
                    
                    // Create a simple object with the raw message as a fallback
                    jsonData = new JsonObject
                    {
                        ["raw_message"] = consumeResult.Message.Value,
                        ["parse_error"] = ex.Message
                    };
                }

                // Add message metadata
                jsonData["_metadata"] = new JsonObject
                {
                    ["topic"] = consumeResult.Topic,
                    ["partition"] = consumeResult.Partition.Value,
                    ["offset"] = consumeResult.Offset.Value
                };

                // Add timestamp if available
                if (consumeResult.Message.Timestamp.Type == TimestampType.CreateTime)
                {
                    ((JsonObject)jsonData["_metadata"])["timestamp"] = 
                        DateTimeOffset.FromUnixTimeMilliseconds(consumeResult.Message.Timestamp.UnixTimestampMs).ToString("o");
                }

                // Create source element with the parsed data
                var data = new SourceElement(elementId, [consumeResult.Topic], jsonData);
                return Task.FromResult(data);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error mapping Avro event for topic {Topic}, partition {Partition}, offset {Offset}", 
                    consumeResult.Topic, consumeResult.Partition.Value, consumeResult.Offset.Value);
                
                // Create a fallback object with error information
                var jsonData = new JsonObject
                {
                    ["error"] = ex.Message,
                    ["topic"] = consumeResult.Topic,
                    ["partition"] = consumeResult.Partition.Value,
                    ["offset"] = consumeResult.Offset.Value
                };
                
                var data = new SourceElement(
                    $"{consumeResult.Topic}-{consumeResult.Partition.Value}-{consumeResult.Offset.Value}", 
                    [consumeResult.Topic], 
                    jsonData);
                
                return Task.FromResult(data);
            }
        }

        /// <summary>
        /// Converts an Avro GenericRecord to a JsonObject
        /// </summary>
        private JsonObject ConvertAvroRecordToJson(GenericRecord record)
        {
            var result = new JsonObject();
            
            foreach (var field in record.Schema.Fields)
            {
                var fieldName = field.Name;
                var fieldValue = record[fieldName];
                
                if (fieldValue == null)
                {
                    result[fieldName] = null;
                    continue;
                }
                
                switch (fieldValue)
                {
                    case GenericRecord nestedRecord:
                        result[fieldName] = ConvertAvroRecordToJson(nestedRecord);
                        break;
                    case Array array:
                        var jsonArray = new JsonArray();
                        foreach (var item in array)
                        {
                            if (item is GenericRecord itemRecord)
                            {
                                jsonArray.Add(ConvertAvroRecordToJson(itemRecord));
                            }
                            else
                            {
                                jsonArray.Add(JsonValue.Create(item));
                            }
                        }
                        result[fieldName] = jsonArray;
                        break;
                    default:
                        result[fieldName] = JsonValue.Create(fieldValue);
                        break;
                }
            }
            
            return result;
        }
    }
}