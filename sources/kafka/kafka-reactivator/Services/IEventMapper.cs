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

using Confluent.Kafka;
using Drasi.Source.SDK.Models;

namespace Reactivator.Services
{
    /// <summary>
    /// Interface for mapping Kafka messages to Drasi SourceChange objects
    /// </summary>
    public interface IEventMapper
    {
        /// <summary>
        /// Maps a Kafka message to a Drasi SourceChange
        /// </summary>
        /// <param name="consumeResult">The Kafka message</param>
        /// <param name="reactivatorStartNs">Timestamp when the reactivator started processing this message (nanoseconds)</param>
        /// <returns>A SourceChange object</returns>
        Task<SourceChange> MapEventAsync(ConsumeResult<string, string> consumeResult, long reactivatorStartNs);
        
        /// <summary>
        /// Gets the format supported by this mapper
        /// </summary>
        string Format { get; }
    }
}