# Kafka Source for Drasi Platform

This implementation adds Kafka support to the Drasi platform as a new source type, following the same architectural patterns as the existing EventHub source.

## Components

### kafka-proxy
The proxy component handles bootstrapping/initial data loading from Kafka topics. It reads messages from specified topics within a configurable time window and publishes them to the Drasi platform for initial data population.

**Configuration:**
- `bootstrapServers`: Kafka broker endpoints
- `bootstrapWindow`: Time window in minutes for initial data loading
- `groupId`: Kafka consumer group ID (defaults to "drasi-kafka-source")
- `securityProtocol`: Security protocol to use (defaults to "PLAINTEXT")
- `saslMechanism`: SASL mechanism to use (defaults to "PLAIN")
- `saslUsername`/`saslPassword`: Authentication credentials
- `sslCaLocation`: SSL CA certificate location
- `sslCertificateLocation`: SSL certificate location
- `sslKeyLocation`: SSL key location
- `sslKeyPassword`: SSL key password
- `topics`: Comma-separated list of Kafka topics to consume
- `messageFormat`: Format of messages (json, avro, text) (defaults to "json")
- `maxBatchSize`: Maximum number of messages to process in a single batch (defaults to 100)
- `deadLetterTopic`: Topic to send failed messages to

### kafka-reactivator  
The reactivator component monitors Kafka topics for real-time changes and publishes them to the Drasi platform as SourceChange events.

**Configuration:**
- `bootstrapServers`: Kafka broker endpoints  
- `groupId`: Kafka consumer group ID (defaults to "drasi-kafka-source")
- `securityProtocol`: Security protocol to use (defaults to "PLAINTEXT")
- `saslMechanism`: SASL mechanism to use (defaults to "PLAIN")
- `saslUsername`/`saslPassword`: Authentication credentials
- `sslCaLocation`: SSL CA certificate location
- `sslCertificateLocation`: SSL certificate location
- `sslKeyLocation`: SSL key location
- `sslKeyPassword`: SSL key password
- `topics`: Comma-separated list of Kafka topics to monitor
- `messageFormat`: Format of messages (json, avro, text) (defaults to "json")
- `maxBatchSize`: Maximum number of messages to process in a single batch (defaults to 100)
- `consumerThreadsPerTopic`: Number of consumer threads per topic (defaults to 1)
- `deadLetterTopic`: Topic to send failed messages to
- `retryPolicy.maxRetryAttempts`: Maximum number of retry attempts (defaults to 5)
- `retryPolicy.initialRetryDelayMs`: Initial retry delay in milliseconds (defaults to 1000)
- `retryPolicy.maxRetryDelayMs`: Maximum retry delay in milliseconds (defaults to 30000)
- `retryPolicy.backoffMultiplier`: Retry backoff multiplier (defaults to 2.0)
- `retryPolicy.circuitBreakerThreshold`: Circuit breaker failure threshold (defaults to 5)
- `retryPolicy.circuitBreakerResetTimeoutMs`: Circuit breaker reset timeout in milliseconds (defaults to 60000)

## Features

- **Authentication Support**: Supports SASL/PLAIN, SASL/SCRAM, and SSL/TLS authentication for secure Kafka clusters
- **Offset Management**: Automatically tracks and resumes from last consumed offsets using Drasi state store
- **Multiple Message Format Support** (both kafka-proxy and kafka-reactivator): 
  - JSON: Maps JSON messages from Kafka to Drasi SourceElement format
  - Avro: Deserializes Avro-formatted messages with schema registry support
  - Plain Text: Handles plain text messages with configurable field mapping
- **Multi-topic Support**: Can consume from multiple Kafka topics simultaneously
- **Configurable Bootstrap Window**: Initial data loading can be configured with time window
- **Advanced Error Handling**: 
  - Exponential backoff retry with configurable parameters
  - Circuit breaker pattern to prevent cascading failures
  - Dead letter queue for failed messages
- **Batch Processing**: Support for processing messages in batches for improved performance
- **Health Monitoring**: Health check endpoints for monitoring Kafka connectivity
- **Observability**: 
  - Detailed metrics collection (consumer lag, processing time, error rates)
  - OpenTelemetry integration for distributed tracing
  - Enhanced logging for troubleshooting
- **Parallel Processing**: Configurable number of consumer threads per topic
- **Graceful Shutdown**: Proper resource cleanup and offset management during shutdown

## Usage

The Kafka source can be deployed alongside other Drasi sources and configured to consume from any Kafka-compatible message broker including:
- Apache Kafka
- Confluent Platform
- Amazon MSK
- Azure Event Hubs with Kafka protocol
- Redpanda

## Docker Images

The implementation includes Docker configurations for:
- `drasi-project/source-kafka-proxy:latest`
- `drasi-project/source-kafka-reactivator:latest`

Both images support multiple platform builds and deployment configurations.

## Dependencies

- .NET 8.0 runtime
- Confluent.Kafka client library v2.6.1
- Apache.Avro v1.11.3
- OpenTelemetry.Extensions.Hosting v1.7.0
- OpenTelemetry.Instrumentation.AspNetCore v1.7.0
- Polly v8.3.0 for resilience patterns
- Drasi.Source.SDK v0.1.8-alpha