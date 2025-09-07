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
    reactivator:
      image: drasi-project/source-kafka-reactivator:latest
      env:
        - name: bootstrapServers
          value: "kafka-broker-1:9092,kafka-broker-2:9092"
        - name: groupId
          value: "drasi-kafka-consumer"
        - name: topics
          value: "orders,customers,products"
```

## Configuration with Authentication

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
        - name: username
          valueFrom:
            secretKeyRef:
              name: kafka-credentials
              key: username
        - name: password
          valueFrom:
            secretKeyRef:
              name: kafka-credentials
              key: password
    reactivator:
      image: drasi-project/source-kafka-reactivator:latest
      env:
        - name: bootstrapServers
          value: "secure-kafka:9093"
        - name: groupId
          value: "drasi-secure-consumer"
        - name: topics
          value: "sensitive-data"
        - name: username
          valueFrom:
            secretKeyRef:
              name: kafka-credentials
              key: username
        - name: password
          valueFrom:
            secretKeyRef:
              name: kafka-credentials
              key: password
```

## Configuration Parameters

### Common Parameters
- `bootstrapServers`: Comma-separated list of Kafka broker addresses
- `topics`: Comma-separated list of Kafka topics to consume
- `groupId`: Kafka consumer group ID (default: "drasi-kafka-source")
- `username`: Optional SASL username for authentication
- `password`: Optional SASL password for authentication

### Proxy-specific Parameters
- `bootstrapWindow`: Time window in minutes for initial data loading (default: 0, disabled)

## Message Format

The Kafka source expects JSON messages in the following format:

```json
{
  "id": "unique-identifier",
  "data": {
    // your message payload
  },
  "timestamp": "2024-01-15T10:30:00Z"
}
```

The source will automatically map:
- Message key → Element ID (or generate from topic-partition-offset)
- Message value → Element data (parsed as JSON)
- Topic name → Element labels
- Message timestamp → Event timestamp