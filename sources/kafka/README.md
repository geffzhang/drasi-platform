# Kafka Source for Drasi Platform

This directory contains the Kafka source implementation for the Drasi platform, providing integration with Apache Kafka message queues.

## Components

### kafka-proxy
Handles bootstrap operations for initial data loading from Kafka topics within a specified time window.

### kafka-reactivator  
Handles real-time monitoring and streaming of changes from Kafka topics.

## Configuration

The Kafka source supports the following configuration options:

### Common Configuration
- `bootstrapServers`: Kafka bootstrap servers (default: "localhost:9092")
- `consumerGroup`: Kafka consumer group name
- `securityProtocol`: Security protocol (PLAINTEXT, SASL_PLAINTEXT, SASL_SSL, SSL)
- `saslMechanism`: SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
- `saslUsername`: SASL username for authentication
- `saslPassword`: SASL password for authentication

### Proxy-specific Configuration
- `bootstrapWindow`: Time window in minutes for bootstrap data loading

### Reactivator-specific Configuration  
- `topics`: Comma-separated list of Kafka topics to monitor

## Features

- Compatible with Kafka 2.6.1+ using Confluent.Kafka client
- Supports multiple authentication mechanisms (SASL, SSL)
- Configurable consumer groups and bootstrap servers
- Handles partitioned topics with parallel processing
- Integrates with Drasi's state management for fault tolerance
- Maps Kafka messages to Drasi data model (SourceElement/SourceChange)

## Usage

The Kafka source follows the same deployment pattern as other Drasi sources. Configure the appropriate connection settings and topic names in your Drasi source definition.

Messages are expected to be in JSON format and will be mapped to Drasi nodes with the topic name as the label.