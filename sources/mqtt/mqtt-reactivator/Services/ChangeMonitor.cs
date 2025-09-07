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
using System.Threading.Channels;
using MQTTnet;
using MQTTnet.Client;
using Drasi.Source.SDK;
using Drasi.Source.SDK.Models;

namespace Reactivator.Services
{
    class ChangeMonitor(IEventMapper eventMapper, IConfiguration configuration, ILogger<ChangeMonitor> logger) : IChangeMonitor
    {
        private long _sequenceNumber = 0;

        public async IAsyncEnumerable<SourceChange> Monitor([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var brokerHost = configuration.GetValue<string>("brokerHost") ?? "localhost";
            var brokerPort = configuration.GetValue<int>("brokerPort", 1883);
            var username = configuration.GetValue<string>("username");
            var password = configuration.GetValue<string>("password");
            var clientId = configuration.GetValue<string>("clientId") ?? $"drasi-mqtt-reactivator-{Guid.NewGuid()}";
            var topics = configuration.GetValue<string>("topics")?.Split(',') ?? [];
            var qos = configuration.GetValue<int>("qos", 1);

            logger.LogInformation("Starting MQTT change monitoring for topics: {Topics}", string.Join(", ", topics));

            var factory = new MqttFactory();
            using var mqttClient = factory.CreateMqttClient();

            var optionsBuilder = new MqttClientOptionsBuilder()
                .WithTcpServer(brokerHost, brokerPort)
                .WithClientId(clientId);

            if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password))
            {
                optionsBuilder.WithCredentials(username, password);
            }

            var options = optionsBuilder.Build();

            await foreach (var change in MonitorMessages(mqttClient, options, topics, qos, cancellationToken))
            {
                yield return change;
            }
        }

        private async IAsyncEnumerable<SourceChange> MonitorMessages(
            IMqttClient mqttClient, 
            MqttClientOptions options, 
            string[] topics, 
            int qos,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var messageQueue = Channel.CreateUnbounded<MqttApplicationMessage>();

            mqttClient.ApplicationMessageReceivedAsync += async e =>
            {
                try
                {
                    await messageQueue.Writer.WriteAsync(e.ApplicationMessage, cancellationToken);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Error writing message to queue");
                }
            };

            mqttClient.DisconnectedAsync += async e =>
            {
                logger.LogWarning("MQTT client disconnected: {Reason}", e.Reason);
                if (!cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(5000, cancellationToken);
                    try
                    {
                        logger.LogInformation("Attempting to reconnect to MQTT broker");
                        await mqttClient.ConnectAsync(options, cancellationToken);
                        await SubscribeToTopics(mqttClient, topics, qos, cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Failed to reconnect to MQTT broker");
                    }
                }
            };

            var connectionEstablished = false;
            try
            {
                logger.LogInformation("Connecting to MQTT broker");
                await mqttClient.ConnectAsync(options, cancellationToken);
                
                await SubscribeToTopics(mqttClient, topics, qos, cancellationToken);

                logger.LogInformation("Successfully connected and subscribed to MQTT topics");
                connectionEstablished = true;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error connecting to MQTT broker");
                messageQueue.Writer.Complete();
                yield break;
            }

            if (connectionEstablished)
            {
                try
                {
                    await foreach (var message in messageQueue.Reader.ReadAllAsync(cancellationToken))
                    {
                        SourceChange? change = null;
                        try
                        {
                            var sequenceNumber = Interlocked.Increment(ref _sequenceNumber);
                            change = await eventMapper.MapEventAsync(message, sequenceNumber);
                        }
                        catch (Exception ex)
                        {
                            logger.LogError(ex, "Error mapping MQTT message to change");
                            continue;
                        }

                        if (change != null)
                        {
                            yield return change;
                        }
                    }
                }
                finally
                {
                    messageQueue.Writer.Complete();
                    if (mqttClient.IsConnected)
                    {
                        try
                        {
                            await mqttClient.DisconnectAsync();
                        }
                        catch (Exception ex)
                        {
                            logger.LogError(ex, "Error disconnecting from MQTT broker");
                        }
                    }
                }
            }
        }

        private async Task SubscribeToTopics(IMqttClient mqttClient, string[] topics, int qos, CancellationToken cancellationToken)
        {
            foreach (var topic in topics)
            {
                logger.LogInformation("Subscribing to topic {Topic} with QoS {Qos}", topic, qos);
                await mqttClient.SubscribeAsync(new MqttTopicFilterBuilder()
                    .WithTopic(topic.Trim())
                    .WithQualityOfServiceLevel((MQTTnet.Protocol.MqttQualityOfServiceLevel)qos)
                    .Build(), cancellationToken);
            }
        }
    }
}