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
    using System.Threading.Tasks;
    using Drasi.Source.SDK;
    
    class DeprovisionHandler(IConfiguration configuration, ILogger<DeprovisionHandler> logger) : IDeprovisionHandler
    {
        private readonly IConfiguration _configuration = configuration;
        private readonly ILogger<DeprovisionHandler> _logger = logger;

        public async Task Deprovision(IStateStore stateStore)
        {
            _logger.LogInformation("Deprovisioning Kafka source...");

            var topics = _configuration["topics"] ?? "";
            var topicList = topics.Split(',', StringSplitOptions.RemoveEmptyEntries);

            foreach (var topic in topicList)
            {
                try
                {
                    _logger.LogInformation("Deprovisioning topic: {Topic}", topic);

                    // For Kafka, we need to clear stored offsets for all partitions
                    // Since we don't have easy access to partition count without creating a consumer,
                    // we'll use a pattern to clear all stored offsets for this topic
                    var client = TopicConsumer.BuildClient(_configuration, _logger);
                    
                    try
                    {
                        client.Subscribe(topic);
                        await Task.Delay(5000); // Allow time for partition assignment
                        
                        var assignment = client.Assignment;
                        foreach (var partition in assignment)
                        {
                            _logger.LogInformation("Deprovisioning partition: {Partition} for topic: {Topic}", 
                                partition.Partition, topic);
                            await stateStore.Delete($"{topic}-{partition.Partition}");
                        }
                    }
                    finally
                    {
                        client.Close();
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error deprovisioning topic {Topic}: {Message}", topic, ex.Message);
                }
            }

            _logger.LogInformation("Kafka source deprovisioning completed");
        }
    }
}