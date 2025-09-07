# MQTT Source for Drasi Platform

This implementation adds MQTT support to the Drasi platform as a new source type, following the same architectural patterns as the existing Kafka source.

## Components

### mqtt-proxy
The proxy component handles bootstrapping/initial data loading from MQTT topics. It connects to an MQTT broker and publishes initial messages to the Drasi platform for initial data population.

**Configuration:**
- `brokerHost`: MQTT broker hostname/IP
- `brokerPort`: MQTT broker port (defaults to 1883)
- `username`/`password`: Optional authentication credentials
- `clientId`: MQTT client identifier
- `topics`: Comma-separated list of MQTT topics to consume
- `retainedOnly`: Whether to only consume retained messages during bootstrap

### mqtt-reactivator  
The reactivator component monitors MQTT topics for real-time messages and publishes them to the Drasi platform as SourceChange events.

**Configuration:**
- `brokerHost`: MQTT broker hostname/IP
- `brokerPort`: MQTT broker port (defaults to 1883)
- `username`/`password`: Optional authentication credentials
- `clientId`: MQTT client identifier
- `topics`: Comma-separated list of MQTT topics to monitor
- `qos`: Quality of Service level (0, 1, or 2)

## Features

- **Authentication Support**: Supports username/password authentication for secure MQTT brokers
- **QoS Support**: Configurable Quality of Service levels
- **JSON Message Support**: Maps JSON messages from MQTT to Drasi SourceElement format
- **Multi-topic Support**: Can consume from multiple MQTT topics simultaneously with wildcard support
- **Retained Message Handling**: Support for MQTT retained messages during bootstrap
- **Error Handling**: Robust error handling with automatic reconnection logic

## Usage

The MQTT source can be deployed alongside other Drasi sources and configured to consume from any MQTT-compatible message broker including:
- Eclipse Mosquitto
- AWS IoT Core
- Azure IoT Hub
- HiveMQ
- EMQX

## Docker Images

The implementation includes Docker configurations for:
- `drasi-project/source-mqtt-proxy:latest`
- `drasi-project/source-mqtt-reactivator:latest`

Both images support multiple platform builds and deployment configurations.

## Dependencies

- .NET 8.0 runtime
- MQTTnet client library v4.3.6
- Drasi.Source.SDK v0.1.4-alpha (proxy) / v0.1.8-alpha (reactivator)