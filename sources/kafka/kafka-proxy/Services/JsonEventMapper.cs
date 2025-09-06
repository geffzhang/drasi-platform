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

namespace Proxy.Services 
{
    using System.Text.Json.Nodes;
    using System.Threading.Tasks;
    using Confluent.Kafka;
    using Drasi.Source.SDK.Models;

    class JsonEventMapper() : IEventMapper
    {
        public Task<SourceElement> MapEventAsync(ConsumeResult<string, string> message)
        {
            var elementId = message.Message.Key ?? $"{message.Topic}-{message.Partition.Value}-{message.Offset.Value}";
            var data = new SourceElement(elementId, [message.Topic], JsonNode.Parse(message.Message.Value)?.AsObject());

            return Task.FromResult(data);
        }
    }
}