# Kafka Source Configuration Example

## Basic Configuration

```yaml
apiVersion: v1
kind: Source
metadata:
  name: my-kafka-source
spec:
  kind: Kafka
  services:
    proxy:
      image: drasi-project/source-kafka-proxy:latest
      env:
        - name: bootstrapServers
          value: "kafka-broker-1:9092,kafka-broker-2:9092"
        - name: bootstrapWindow
          value: "30"  # 30 minutes
        - name: groupId
          value: "drasi-kafka-consumer"
        - name: topics
          value: "orders,customers,products"
        - name: messageFormat
          value: "json"  # Options: json, avro, text
    reactivator:
      image: drasi-project/source-kafka-reactivator:latest
      env:
        - name: bootstrapServers
          value: "kafka-broker-1:9092,kafka-broker-2:9092"
        - name: groupId
          value: "drasi-kafka-consumer"
        - name: topics
          value: "orders,customers,products"
        - name: messageFormat
          value: "json"  # Options: json, avro, text
        - name: maxBatchSize
          value: "100"
```

## Advanced Configuration with Security and Error Handling

```yaml
apiVersion: v1
kind: Source
metadata:
  name: my-secure-kafka-source
spec:
  kind: Kafka
  services:
    proxy:
      image: drasi-project/source-kafka-proxy:latest
      env:
        - name: bootstrapServers
          value: "secure-kafka:9093"
        - name: bootstrapWindow
          value: "60"
        - name: groupId
          value: "drasi-secure-consumer"
        - name: topics
          value: "sensitive-data"
        - name: messageFormat
          value: "avro"
        - name: avroSchema
          valueFrom:
            configMapKeyRef:
              name: kafka-schemas
              key: sensitive-data-schema
        - name: maxBatchSize
          value: "200"
        # Security settings
        - name: securityProtocol
          value: "SASL_SSL"
        - name: saslMechanism
          value: "SCRAM-SHA-512"
        - name: saslUsername
          valueFrom:
            secretKeyRef:
              name: kafka-credentials
              key: username
        - name: saslPassword
          valueFrom:
            secretKeyRef:
              name: kafka-credentials
              key: password
        - name: sslCaLocation
          value: "/etc/kafka/certs/ca.pem"
        # Error handling
        - name: deadLetterTopic
          value: "drasi-dead-letter"
    reactivator:
      image: drasi-project/source-kafka-reactivator:latest
      env:
        - name: bootstrapServers
          value: "secure-kafka:9093"
        - name: groupId
          value: "drasi-secure-consumer"
        - name: topics
          value: "sensitive-data"
        - name: messageFormat
          value: "avro"
        - name: avroSchema
          valueFrom:
            configMapKeyRef:
              name: kafka-schemas
              key: sensitive-data-schema
        # Security settings
        - name: securityProtocol
          value: "SASL_SSL"
        - name: saslMechanism
          value: "SCRAM-SHA-512"
        - name: saslUsername
          valueFrom:
            secretKeyRef:
              name: kafka-credentials
              key: username
        - name: saslPassword
          valueFrom:
            secretKeyRef:
              name: kafka-credentials
              key: password
        - name: sslCaLocation
          value: "/etc/kafka/certs/ca.pem"
        # Performance settings
        - name: maxBatchSize
          value: "200"
        - name: consumerThreadsPerTopic
          value: "3"
        # Error handling
        - name: deadLetterTopic
          value: "drasi-dead-letter"
        - name: retryPolicy.maxRetryAttempts
          value: "5"
        - name: retryPolicy.initialRetryDelayMs
          value: "1000"
        - name: retryPolicy.maxRetryDelayMs
          value: "30000"
        - name: retryPolicy.backoffMultiplier
          value: "2.0"
```

## Plain Text Message Format Example

```yaml
apiVersion: v1
kind: Source
metadata:
  name: my-text-kafka-source
spec:
  kind: Kafka
  services:
    proxy:
      image: drasi-project/source-kafka-proxy:latest
      env:
        - name: bootstrapServers
          value: "kafka-broker-1:9092"
        - name: topics
          value: "log-data"
        - name: messageFormat
          value: "text"
        - name: textFieldNames
          value: "timestamp,level,service,message"
        - name: textDelimiter
          value: "|"
    reactivator:
      image: drasi-project/source-kafka-reactivator:latest
      env:
        - name: bootstrapServers
          value: "kafka-broker-1:9092"
        - name: topics
          value: "log-data"
        - name: messageFormat
          value: "text"
        - name: textFieldNames
          value: "timestamp,level,service,message"
        - name: textDelimiter
          value: "|"
```

## Configuration Parameters

### Common Parameters
- `bootstrapServers`: Comma-separated list of Kafka broker addresses
- `topics`: Comma-separated list of Kafka topics to consume
- `groupId`: Kafka consumer group ID (default: "drasi-kafka-source")
- `messageFormat`: Format of messages (default: "json", options: "json", "avro", "text")
- `maxBatchSize`: Maximum number of messages to process in a single batch (default: 100)

### Security Parameters
- `securityProtocol`: Security protocol to use (default: "PLAINTEXT", options: "PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL")
- `saslMechanism`: SASL mechanism to use (default: "PLAIN", options: "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512")
- `saslUsername`/`saslPassword`: Authentication credentials
- `sslCaLocation`: SSL CA certificate location
- `sslCertificateLocation`: SSL certificate location
- `sslKeyLocation`: SSL key location
- `sslKeyPassword`: SSL key password

### Message Format Parameters
- **JSON Format**: Default, no additional parameters needed
- **Avro Format**:
  - `avroSchema`: JSON string of the Avro schema
- **Text Format**:
  - `textFieldNames`: Comma-separated list of field names to map text values to (default: "value")
  - `textDelimiter`: Delimiter to split text messages (default: "\t")

### Proxy-specific Parameters
- `bootstrapWindow`: Time window in minutes for initial data loading (default: 0, disabled)

### Reactivator-specific Parameters
- `consumerThreadsPerTopic`: Number of consumer threads per topic (default: 1)
- `retryPolicy.maxRetryAttempts`: Maximum number of retry attempts (default: 5)
- `retryPolicy.initialRetryDelayMs`: Initial retry delay in milliseconds (default: 1000)
- `retryPolicy.maxRetryDelayMs`: Maximum retry delay in milliseconds (default: 30000)
- `retryPolicy.backoffMultiplier`: Retry backoff multiplier (default: 2.0)
- `retryPolicy.circuitBreakerThreshold`: Circuit breaker failure threshold (default: 5)
- `retryPolicy.circuitBreakerResetTimeoutMs`: Circuit breaker reset timeout in milliseconds (default: 60000)

### Error Handling Parameters
- `deadLetterTopic`: Topic to send failed messages to

## Supported Message Formats

### JSON Format
The Kafka source can process JSON messages in any format. Example:

```json
{
  "id": "order-12345",
  "customer": "ACME Corp",
  "items": [
    {"product": "Widget A", "quantity": 5},
    {"product": "Widget B", "quantity": 2}
  ],
  "timestamp": "2024-01-15T10:30:00Z"
}
```

### Avro Format
The Kafka source can process Avro-formatted messages using a provided schema. Example schema:

```json
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "customer", "type": "string"},
    {"name": "items", "type": {"type": "array", "items": {
      "type": "record",
      "name": "OrderItem",
      "fields": [
        {"name": "product", "type": "string"},
        {"name": "quantity", "type": "int"}
      ]
    }}},
    {"name": "timestamp", "type": "string"}
  ]
}
```

### Plain Text Format
The Kafka source can process plain text messages with configurable field mapping. Example:

```
2024-01-15T10:30:00Z|INFO|OrderService|Order 12345 processed successfully
```

With `textFieldNames=timestamp,level,service,message` and `textDelimiter=|`, this would be mapped to:

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "level": "INFO",
  "service": "OrderService",
  "message": "Order 12345 processed successfully"
}
```

## Data Mapping

The source will automatically map:
- Message key → Element ID (or generate from topic-partition-offset)
- Message value → Element data (parsed according to messageFormat)
- Topic name → Element labels
- Message timestamp → Event timestamp (if available)
- Message metadata → Added to _metadata field