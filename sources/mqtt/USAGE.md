# MQTT Source Usage Guide

This document provides detailed configuration and usage information for the MQTT source components.

## Configuration Parameters

### Common Parameters (both proxy and reactivator)

#### Connection Configuration
- `brokerHost`: MQTT broker hostname or IP address (required)
- `brokerPort`: MQTT broker port (default: 1883)
- `useTls`: Enable TLS/SSL encryption (default: false)
- `tlsCertPath`: Path to client certificate file (optional)
- `tlsKeyPath`: Path to client private key file (optional)
- `caCertPath`: Path to CA certificate file for server validation (optional)
- `clientId`: MQTT client identifier (default: auto-generated)

#### Authentication
- `username`: MQTT username (optional)
- `password`: MQTT password (optional)
- `authMethod`: Authentication method (none, password, certificate)

#### Message Format Configuration
- `messageFormat`: Message format (json, text, binary) - defaults to auto-detection
- `textFieldNames`: Comma-separated field names for text parsing (default: "value")
- `textDelimiter`: Delimiter for text message parsing (default: "\t")

#### Performance and Error Handling
- `maxRetryAttempts`: Maximum number of retry attempts (default: 5)
- `retryDelayMs`: Initial retry delay in milliseconds (default: 1000)
- `circuitBreakerThreshold`: Number of failures before opening circuit (default: 5)
- `circuitBreakerTimeoutMs`: Circuit breaker timeout in milliseconds (default: 30000)
- `batchSize`: Maximum number of messages to process in batch (default: 100)
- `batchTimeoutMs`: Maximum time to wait for batch completion (default: 1000)

#### Monitoring and Health Checks
- `healthCheckPort`: Port for health check endpoint (default: 8080)
- `metricsPort`: Port for metrics endpoint (default: 9090)
- `enableOpenTelemetry`: Enable OpenTelemetry tracing (default: true)
- `otelEndpoint`: OpenTelemetry collector endpoint (optional)

### Proxy-Specific Parameters
- `topics`: Comma-separated list of MQTT topics to consume during bootstrap
- `retainedOnly`: Whether to only consume retained messages during bootstrap (default: false)
- `bootstrapTimeoutMs`: Timeout for bootstrap operation (default: 30000)

### Reactivator-Specific Parameters
- `topics`: Comma-separated list of MQTT topics to monitor for real-time changes
- `qos`: Quality of Service level (0, 1, or 2) (default: 1)
- `autoReconnect`: Enable automatic reconnection on connection loss (default: true)
- `reconnectDelayMs`: Delay between reconnection attempts (default: 5000)

## Environment Variables

All configuration parameters can be set using environment variables with the `MQTT_` prefix:

```bash
export MQTT_BROKER_HOST=broker.example.com
export MQTT_BROKER_PORT=8883
export MQTT_USE_TLS=true
export MQTT_USERNAME=myuser
export MQTT_PASSWORD=mypassword
export MQTT_MESSAGE_FORMAT=json
export MQTT_MAX_RETRY_ATTEMPTS=10
```

## Configuration File

Alternatively, you can use a JSON configuration file:

```json
{
  "brokerHost": "broker.example.com",
  "brokerPort": 8883,
  "useTls": true,
  "username": "myuser",
  "password": "mypassword",
  "messageFormat": "json",
  "maxRetryAttempts": 10,
  "healthCheckPort": 8080,
  "topics": "sensors/+, devices/#"
}
```

## Message Format Examples

### JSON Format
```json
{
  "sensorId": "sensor-001",
  "temperature": 23.5,
  "humidity": 45.2,
  "timestamp": "2024-01-15T10:30:00Z"
}
```

### Text Format (with field mapping)
```
sensor-001	23.5	45.2	2024-01-15T10:30:00Z
```

Configure with:
```bash
export MQTT_MESSAGE_FORMAT=text
export MQTT_TEXT_FIELD_NAMES=sensorId,temperature,humidity,timestamp
export MQTT_TEXT_DELIMITER="\t"
```

### Binary Format
Binary messages will include metadata about the message size and optional base64 encoding for debugging.

## Health Checks

The components expose health check endpoints:

- **Health**: `http://localhost:8080/health`
- **Metrics**: `http://localhost:9090/metrics`
- **Ready**: `http://localhost:8080/ready`

## Monitoring and Observability

### Metrics
- `mqtt_messages_received_total`: Total messages received
- `mqtt_messages_processed_total`: Total messages processed successfully
- `mqtt_message_processing_duration_seconds`: Message processing duration histogram
- `mqtt_errors_total`: Total processing errors by type
- `mqtt_connection_status`: MQTT broker connection status

### Tracing
OpenTelemetry traces are automatically generated for:
- Message processing
- MQTT connection events
- Error handling and retries

## Error Handling

The implementation includes comprehensive error handling:

1. **Retry Policy**: Exponential backoff with configurable attempts and delays
2. **Circuit Breaker**: Prevents cascading failures when downstream services are unavailable
3. **Dead Letter Handling**: Failed messages can be routed to a dead letter topic
4. **Connection Recovery**: Automatic reconnection with configurable delays

## Security

### TLS/SSL Encryption
```bash
export MQTT_USE_TLS=true
export MQTT_CA_CERT_PATH=/certs/ca.crt
export MQTT_TLS_CERT_PATH=/certs/client.crt
export MQTT_TLS_KEY_PATH=/certs/client.key
```

### Authentication Methods
- Password authentication (username/password)
- Client certificate authentication
- Anonymous access (not recommended for production)

## Performance Tuning

### Batch Processing
```bash
export MQTT_BATCH_SIZE=500
export MQTT_BATCH_TIMEOUT_MS=2000
```

### Connection Pooling
```bash
export MQTT_MAX_CONNECTIONS=10
export MQTT_CONNECTION_TIMEOUT_MS=5000
```

## Deployment Examples

### Docker Compose
```yaml
version: '3.8'
services:
  mqtt-proxy:
    image: drasi-project/source-mqtt-proxy:latest
    environment:
      - MQTT_BROKER_HOST=mosquitto
      - MQTT_BROKER_PORT=1883
      - MQTT_TOPICS=sensors/+,devices/#
      - MQTT_MESSAGE_FORMAT=json
    ports:
      - "8080:8080"
      - "9090:9090"

  mqtt-reactivator:
    image: drasi-project/source-mqtt-reactivator:latest
    environment:
      - MQTT_BROKER_HOST=mosquitto
      - MQTT_BROKER_PORT=1883
      - MQTT_TOPICS=sensors/+,devices/#
      - MQTT_QOS=1
    ports:
      - "8081:8080"
      - "9091:9090"
```

### Kubernetes Deployment
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mqtt-reactivator
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: mqtt-reactivator
        image: drasi-project/source-mqtt-reactivator:latest
        env:
        - name: MQTT_BROKER_HOST
          value: "mqtt-broker"
        - name: MQTT_BROKER_PORT
          value: "1883"
        - name: MQTT_TOPICS
          value: "sensors/+,devices/#"
        ports:
        - containerPort: 8080
        - containerPort: 9090
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
```

## Troubleshooting

### Common Issues

1. **Connection Timeouts**: Check network connectivity and firewall rules
2. **Authentication Failures**: Verify credentials and certificate validity
3. **Message Processing Errors**: Check message format compatibility
4. **Memory Issues**: Adjust batch size and timeout settings

### Logging
Enable debug logging for troubleshooting:
```bash
export LOG_LEVEL=Debug
```

Logs will include detailed information about:
- Connection events
- Message processing
- Error conditions
- Performance metrics