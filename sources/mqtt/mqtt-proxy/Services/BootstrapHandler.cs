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

using System.Runtime.CompilerServices;
using MQTTnet;
using MQTTnet.Client;
using Drasi.Source.SDK;
using Drasi.Source.SDK.Models;

namespace Proxy.Services
{
    class BootstrapHandler(IEventMapper eventMapper, IConfiguration configuration, ILogger<BootstrapHandler> logger): IBootstrapHandler
    {
        public async IAsyncEnumerable<SourceElement> Bootstrap(BootstrapRequest request, [EnumeratorCancellation]CancellationToken cancellationToken = default)
        {
            var brokerHost = configuration.GetValue<string>("brokerHost") ?? "localhost";
            var brokerPort = configuration.GetValue<int>("brokerPort", 1883);
            var username = configuration.GetValue<string>("username");
            var password = configuration.GetValue<string>("password");
            var clientId = configuration.GetValue<string>("clientId") ?? $"drasi-mqtt-proxy-{Guid.NewGuid()}";
            var retainedOnly = configuration.GetValue<bool>("retainedOnly", true);
            var bootstrapTimeoutMs = configuration.GetValue<int>("bootstrapTimeoutMs", 10000);

            logger.LogInformation("Starting MQTT bootstrap for {Topics}", string.Join(", ", request.NodeLabels));

            foreach (var topic in request.NodeLabels)
            {
                await foreach (var element in GetRetainedMessages(brokerHost, brokerPort, username, password, clientId, topic, retainedOnly, bootstrapTimeoutMs, cancellationToken))
                {
                    yield return element;
                }
            }
        }

        private async IAsyncEnumerable<SourceElement> GetRetainedMessages(
            string brokerHost, 
            int brokerPort, 
            string? username, 
            string? password, 
            string clientId, 
            string topic, 
            bool retainedOnly, 
            int timeoutMs,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var factory = new MqttFactory();
            using var mqttClient = factory.CreateMqttClient();
            
            var receivedMessages = new List<MqttApplicationMessage>();

            mqttClient.ApplicationMessageReceivedAsync += async e =>
            {
                if (!retainedOnly || e.ApplicationMessage.Retain)
                {
                    receivedMessages.Add(e.ApplicationMessage);
                    logger.LogDebug("Received message on topic {Topic}", e.ApplicationMessage.Topic);
                }
                await Task.CompletedTask;
            };

            var optionsBuilder = new MqttClientOptionsBuilder()
                .WithTcpServer(brokerHost, brokerPort)
                .WithClientId(clientId);

            if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password))
            {
                optionsBuilder.WithCredentials(username, password);
            }

            var options = optionsBuilder.Build();

            try
            {
                logger.LogInformation("Connecting to MQTT broker at {Host}:{Port}", brokerHost, brokerPort);
                await mqttClient.ConnectAsync(options, cancellationToken);
                
                logger.LogInformation("Subscribing to topic {Topic}", topic);
                await mqttClient.SubscribeAsync(new MqttTopicFilterBuilder()
                    .WithTopic(topic)
                    .Build(), cancellationToken);

                // Wait for messages or timeout
                await Task.Delay(timeoutMs, cancellationToken);

                logger.LogInformation("Bootstrap completed for topic {Topic}, found {Count} messages", topic, receivedMessages.Count);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error during MQTT bootstrap for topic {Topic}", topic);
                yield break;
            }
            finally
            {
                if (mqttClient.IsConnected)
                {
                    await mqttClient.DisconnectAsync();
                }
            }

            // Yield messages after try-catch block
            foreach (var message in receivedMessages)
            {
                if (cancellationToken.IsCancellationRequested)
                    yield break;

                var element = await eventMapper.MapEventAsync(message);
                yield return element;
            }
        }       
    }
}