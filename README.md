# Kafka Playground

A playground application for kafka

- producer
- consumer
- stream
- ktable

with spring framework, avro serializers and build with gradle.

## Prerequisites
- JDK17
- Docker

## Before
Start kafka with:
```bash
docker compose -f confluent-platform/docker-compose.yml up -d
```

## Build
```bash
./gradlew clean build
```

## Run
### kafka producer
```bash
./gradlew :kafka-producer:bootRun
```
### kafka consumer
```bash
./gradlew :kafka-consumer:bootRun
```
### kafka streams
```bash
./gradlew :kafka-streams:bootRun
```