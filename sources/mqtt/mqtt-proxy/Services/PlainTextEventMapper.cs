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
using System.Text;
using MQTTnet;
using Drasi.Source.SDK.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Proxy.Services 
{
    /// <summary>
    /// Maps plain text messages from MQTT to Drasi SourceElement objects
    /// </summary>
    public class PlainTextEventMapper : IEventMapper
    {
        private readonly ILogger<PlainTextEventMapper> _logger;
        private readonly IConfiguration _configuration;
        private readonly string[] _fieldNames;
        private readonly string _delimiter;

        public string Format => "text";

        public PlainTextEventMapper(
            ILogger<PlainTextEventMapper> logger,
            IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
            
            // Get field names from configuration
            var fieldNamesConfig = _configuration.GetValue<string>("textFieldNames", "value");
            _fieldNames = fieldNamesConfig.Split(',', StringSplitOptions.TrimEntries);
            
            // Get delimiter from configuration
            _delimiter = _configuration.GetValue<string>("textDelimiter", "\t");
        }

        public Task<SourceElement> MapEventAsync(MqttApplicationMessage message)
        {
            try
            {
                // Generate element ID from topic and timestamp
                var elementId = $"{message.Topic}-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
                
                // Parse text message
                JsonObject jsonData = new JsonObject();
                
                try
                {
                    var messagePayload = Encoding.UTF8.GetString(message.PayloadSegment);
                    
                    // If delimiter is specified and we have field names, try to split the message
                    if (!string.IsNullOrEmpty(_delimiter) && _fieldNames.Length > 0)
                    {
                        var values = messagePayload.Split(_delimiter);
                        
                        // Map values to field names
                        for (int i = 0; i < Math.Min(_fieldNames.Length, values.Length); i++)
                        {
                            jsonData[_fieldNames[i]] = values[i];
                        }
                        
                        // If we have more values than field names, add them with generic names
                        for (int i = _fieldNames.Length; i < values.Length; i++)
                        {
                            jsonData[$"field{i}"] = values[i];
                        }
                    }
                    else
                    {
                        // If no delimiter or field names, just use the raw message
                        jsonData["value"] = messagePayload;
                    }
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(ex, "Failed to parse message as text for topic {Topic}", message.Topic);
                    
                    // Create a simple object with the raw message as a fallback
                    jsonData = new JsonObject
                    {
                        ["raw_message"] = Encoding.UTF8.GetString(message.PayloadSegment),
                        ["parse_error"] = ex.Message
                    };
                }

                // Add message metadata
                jsonData["_metadata"] = new JsonObject
                {
                    ["topic"] = message.Topic,
                    ["qos"] = message.QualityOfServiceLevel.ToString(),
                    ["retain"] = message.Retain
                };

                // Add timestamp
                ((JsonObject)jsonData["_metadata"])["timestamp"] = DateTimeOffset.UtcNow.ToString("o");

                // Create source element with the parsed data
                var data = new SourceElement(elementId, [message.Topic], jsonData);
                return Task.FromResult(data);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error mapping text event for topic {Topic}", message.Topic);
                
                // Create a fallback object with error information
                var jsonData = new JsonObject
                {
                    ["error"] = ex.Message,
                    ["topic"] = message.Topic
                };
                
                var data = new SourceElement(
                    $"{message.Topic}-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}", 
                    [message.Topic], 
                    jsonData);
                
                return Task.FromResult(data);
            }
        }
    }
}