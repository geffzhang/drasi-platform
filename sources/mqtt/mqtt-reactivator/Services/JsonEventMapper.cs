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

namespace Reactivator.Services
{
    using System.Text.Json.Nodes;
    using System.Threading.Tasks;
    using System.Text;
    using MQTTnet;
    using Drasi.Source.SDK.Models;

    class JsonEventMapper() : IEventMapper
    {
        public Task<SourceChange> MapEventAsync(MqttApplicationMessage message, long sequenceNumber)
        {
            var elementId = $"{message.Topic}-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
            var messagePayload = Encoding.UTF8.GetString(message.PayloadSegment);
            
            JsonObject? jsonData = null;
            try 
            {
                jsonData = JsonNode.Parse(messagePayload)?.AsObject();
            }
            catch
            {
                // If not valid JSON, create a simple object with the raw payload
                jsonData = new JsonObject
                {
                    ["payload"] = messagePayload,
                    ["topic"] = message.Topic,
                    ["timestamp"] = DateTimeOffset.UtcNow.ToString("O")
                };
            }

            var element = new SourceElement(elementId, [message.Topic], jsonData);
            var timestampNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000; // Convert to nanoseconds
            var reactivatorStartNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000;
            
            var change = new SourceChange(ChangeOp.INSERT, element, timestampNs, reactivatorStartNs, sequenceNumber);
            return Task.FromResult(change);
        }
    }
}