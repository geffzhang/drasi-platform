# Kafka Source for Drasi Platform

This implementation adds Kafka support to the Drasi platform as a new source type, following the same architectural patterns as the existing EventHub source.

## Components

### kafka-proxy
The proxy component handles bootstrapping/initial data loading from Kafka topics. It reads messages from specified topics within a configurable time window and publishes them to the Drasi platform for initial data population.

**Configuration:**
- `bootstrapServers`: Kafka broker endpoints
- `bootstrapWindow`: Time window in minutes for initial data loading
- `groupId`: Kafka consumer group ID (defaults to "drasi-kafka-source")
- `username`/`password`: Optional authentication credentials
- `topics`: Comma-separated list of Kafka topics to consume

### kafka-reactivator  
The reactivator component monitors Kafka topics for real-time changes and publishes them to the Drasi platform as SourceChange events.

**Configuration:**
- `bootstrapServers`: Kafka broker endpoints  
- `groupId`: Kafka consumer group ID (defaults to "drasi-kafka-source")
- `username`/`password`: Optional authentication credentials
- `topics`: Comma-separated list of Kafka topics to monitor

## Features

- **Authentication Support**: Supports SASL/PLAIN authentication for secure Kafka clusters
- **Offset Management**: Automatically tracks and resumes from last consumed offsets using Drasi state store
- **JSON Message Support**: Maps JSON messages from Kafka to Drasi SourceElement format
- **Multi-topic Support**: Can consume from multiple Kafka topics simultaneously
- **Configurable Bootstrap Window**: Initial data loading can be configured with time window
- **Error Handling**: Robust error handling with automatic retry logic

## Usage

The Kafka source can be deployed alongside other Drasi sources and configured to consume from any Kafka-compatible message broker including:
- Apache Kafka
- Confluent Platform
- Amazon MSK
- Azure Event Hubs with Kafka protocol

## Docker Images

The implementation includes Docker configurations for:
- `drasi-project/source-kafka-proxy:latest`
- `drasi-project/source-kafka-reactivator:latest`

Both images support multiple platform builds and deployment configurations.

## Dependencies

- .NET 8.0 runtime
- Confluent.Kafka client library v2.6.1
- Drasi.Source.SDK v0.1.4-alpha (proxy) / v0.1.8-alpha (reactivator)