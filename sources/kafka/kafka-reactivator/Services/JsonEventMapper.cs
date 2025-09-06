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
        public Task<SourceChange> MapEventAsync(ConsumeResult<string, string> consumeResult, long reactivatorStartNs)
        {
            var elementId = consumeResult.Message.Key ?? $"{consumeResult.Topic}-{consumeResult.Partition.Value}-{consumeResult.Offset.Value}";
            var data = new SourceElement(elementId, [consumeResult.Topic], JsonNode.Parse(consumeResult.Message.Value)?.AsObject());

            // Convert Kafka timestamp to nanoseconds - if timestamp is available, use it, otherwise use current time
            var eventTimeNs = consumeResult.Message.Timestamp.Type == TimestampType.CreateTime
                ? consumeResult.Message.Timestamp.UnixTimestampMs * 1000000
                : reactivatorStartNs;

            return Task.FromResult(new SourceChange(ChangeOp.INSERT, data, eventTimeNs, reactivatorStartNs, consumeResult.Offset.Value, consumeResult.Partition.Value.ToString()));
        }
    }
}