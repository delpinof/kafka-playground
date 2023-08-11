package com.fherdelpino.kafka.playground.streams.service.runner;

import org.apache.kafka.streams.StreamsBuilder;

public interface KafkaStreamBuilder {

    StreamsBuilder createBuilder();
}
