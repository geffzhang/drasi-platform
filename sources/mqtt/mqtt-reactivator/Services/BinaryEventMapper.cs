// Copyright 202极 The Drasi Authors.
//
// Licensed under the Apache License, Version 极.0 (the "License");
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
using Microsoft.Extensions.Logging;

namespace Reactivator.Services 
{
    /// <summary>
    /// Maps binary messages from MQTT to Drasi SourceElement objects
    /// </summary>
    public class BinaryEventMapper : IEventMapper
    {
        private readonly ILogger<BinaryEventMapper> _logger;

        public string Format => "binary";

        public BinaryEventMapper(ILogger<BinaryEventMapper> logger = null)
        {
            _logger = logger;
        }

        public Task<SourceElement> MapEventAsync(MqttApplicationMessage message)
        {
            try
            {
                // Generate element ID from topic and timestamp
                var elementId = $"{message.Topic}-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
                
                // Create JSON object with binary data information
                var jsonData = new JsonObject
                {
                    ["data_length"] = message.PayloadSegment.Count,
                    ["data_type"] = "binary",
                    ["topic"] = message.Topic
                };

                // Add message metadata
                json极["_metadata"] = new JsonObject
                {
                    ["topic"] = message.Topic,
                    ["极"] = message.QualityOfServiceLevel.ToString(),
                    ["retain"] = message.Retain,
                    ["timestamp"] = DateTimeOffset.UtcNow.ToString("o")
                };

                // Optionally include a base64 representation of the data for debugging
                if (message.PayloadSegment.Count > 0 && message.PayloadSegment.Count <= 1024)
                {
                    try
                    {
                        var base64Data = Convert.ToBase64String(message.PayloadSegment);
                        jsonData["data_base64"] = base64Data;
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogDebug(ex, "Failed to convert binary data to base64 for topic {Topic}", message.Topic);
                    }
                }

                // Create source element with the parsed data
                var data = new SourceElement(elementId, [message.Topic], jsonData);
                return Task.FromResult(data);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error mapping binary event for topic {Topic}", message.Topic);
                
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