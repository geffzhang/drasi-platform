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
    using Confluent.Kafka;
    using Drasi.Source.SDK.Models;

    class JsonEventMapper() : IEventMapper
    {
        public Task<SourceChange> MapEventAsync(ConsumeResult<string, string> message, long reactivatorStartNs)
        {
            var elementId = message.Message.Key ?? $"{message.Topic}-{message.Partition.Value}-{message.Offset.Value}";
            var element = new SourceElement(elementId, [message.Topic], JsonNode.Parse(message.Message.Value)?.AsObject());
            
            // Convert Kafka offset to LSN
            var lsn = message.Offset.Value;
            
            // Extract timestamp from message or use current time
            var timestampNs = message.Message.Timestamp.Type == TimestampType.CreateTime
                ? message.Message.Timestamp.UnixTimestampMs * 1_000_000L  // Convert ms to ns
                : (DateTimeOffset.UtcNow.Ticks - DateTimeOffset.UnixEpoch.Ticks) * 100; // Current time in ns

            var partition = $"{message.Topic}-{message.Partition.Value}";
            
            // For simplicity, treating all messages as INSERT operations
            // In a real implementation, you might want to parse the message to determine the operation type
            var change = new SourceChange(ChangeOp.INSERT, element, timestampNs, reactivatorStartNs, lsn, partition);
            
            return Task.FromResult(change);
        }
    }
}