# MQTT Source Usage

This document describes how to use the MQTT source implementation for Drasi platform.

## Prerequisites

- MQTT broker (e.g., Eclipse Mosquitto, HiveMQ, AWS IoT Core, Azure IoT Hub)
- Drasi platform running in your environment

## Configuration

### MQTT Proxy Configuration

The MQTT proxy handles bootstrapping from MQTT topics. Configure using environment variables:

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `brokerHost` | MQTT broker hostname/IP | `localhost` | Yes |
| `brokerPort` | MQTT broker port | `1883` | No |
| `username` | MQTT username for authentication | - | No |
| `password` | MQTT password for authentication | - | No |
| `clientId` | MQTT client identifier | `drasi-mqtt-proxy-{guid}` | No |
| `retainedOnly` | Only consume retained messages during bootstrap | `true` | No |
| `bootstrapTimeoutMs` | Timeout for bootstrap in milliseconds | `10000` | No |

### MQTT Reactivator Configuration

The MQTT reactivator monitors MQTT topics for real-time changes. Configure using environment variables:

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `brokerHost` | MQTT broker hostname/IP | `localhost` | Yes |
| `brokerPort` | MQTT broker port | `1883` | No |
| `username` | MQTT username for authentication | - | No |
| `password` | MQTT password for authentication | - | No |
| `clientId` | MQTT client identifier | `drasi-mqtt-reactivator-{guid}` | No |
| `topics` | Comma-separated list of MQTT topics to monitor | - | Yes |
| `qos` | Quality of Service level (0, 1, or 2) | `1` | No |

## Topic Configuration

Topics are specified as node labels in the Drasi source configuration. The MQTT source supports:

- Exact topic names: `sensor/temperature`
- MQTT wildcards: `sensor/+/temperature`, `sensor/#`

## Example Deployment

### Using Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mqtt-source-proxy
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mqtt-source-proxy
  template:
    metadata:
      labels:
        app: mqtt-source-proxy
    spec:
      containers:
      - name: proxy
        image: drasi-project/source-mqtt-proxy:latest
        env:
        - name: brokerHost
          value: "mqtt-broker.example.com"
        - name: brokerPort
          value: "1883"
        - name: username
          value: "mqtt-user"
        - name: password
          valueFrom:
            secretKeyRef:
              name: mqtt-credentials
              key: password
        - name: retainedOnly
          value: "true"
        - name: bootstrapTimeoutMs
          value: "30000"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mqtt-source-reactivator
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mqtt-source-reactivator
  template:
    metadata:
      labels:
        app: mqtt-source-reactivator
    spec:
      containers:
      - name: reactivator
        image: drasi-project/source-mqtt-reactivator:latest
        env:
        - name: brokerHost
          value: "mqtt-broker.example.com"
        - name: brokerPort
          value: "1883"
        - name: username
          value: "mqtt-user"
        - name: password
          valueFrom:
            secretKeyRef:
              name: mqtt-credentials
              key: password
        - name: topics
          value: "sensor/+/temperature,device/+/status"
        - name: qos
          value: "1"
```

### Using Docker Compose

```yaml
version: '3.8'
services:
  mqtt-proxy:
    image: drasi-project/source-mqtt-proxy:latest
    environment:
      - brokerHost=mqtt-broker
      - brokerPort=1883
      - username=mqtt-user
      - password=mqtt-password
      - retainedOnly=true
      - bootstrapTimeoutMs=30000
    depends_on:
      - mqtt-broker

  mqtt-reactivator:
    image: drasi-project/source-mqtt-reactivator:latest
    environment:
      - brokerHost=mqtt-broker
      - brokerPort=1883
      - username=mqtt-user
      - password=mqtt-password
      - topics=sensor/+/temperature,device/+/status
      - qos=1
    depends_on:
      - mqtt-broker

  mqtt-broker:
    image: eclipse-mosquitto:latest
    ports:
      - "1883:1883"
    volumes:
      - ./mosquitto.conf:/mosquitto/config/mosquitto.conf
```

## Message Format

The MQTT source expects JSON messages on subscribed topics. Messages that are not valid JSON will be wrapped in a simple object:

```json
{
  "payload": "original-message-content",
  "topic": "sensor/temperature",
  "timestamp": "2024-01-01T12:00:00.000Z"
}
```

Valid JSON messages are passed through directly as SourceElement properties.

## Security Considerations

- Use username/password authentication when connecting to MQTT brokers
- Consider using TLS/SSL connections (port 8883) for production deployments
- Implement proper MQTT topic permissions on the broker side
- Use strong, unique client IDs to avoid conflicts

## Troubleshooting

### Common Issues

1. **Connection refused**: Check broker host, port, and network connectivity
2. **Authentication failed**: Verify username and password
3. **No messages received**: Check topic subscription and message publication
4. **Client ID conflicts**: Ensure unique client IDs across deployments

### Logging

Both proxy and reactivator provide detailed logging:
- Connection events
- Subscription confirmations
- Message processing
- Error conditions
- Reconnection attempts

Check container logs for debugging:
```bash
kubectl logs deployment/mqtt-source-proxy
kubectl logs deployment/mqtt-source-reactivator
```