package com.fherdelpino.kafka.playground.streams.service.runner;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Slf4j
@RequiredArgsConstructor
@Component
@ConditionalOnProperty(prefix = "playground", name = "stream-type", havingValue = "string")
public class KafkaPlaygroundStringStreamsCommandLineRunner implements CommandLineRunner, TopologyBuilder {

    @Autowired
    private final Properties streamProperties;

    @Value("${kafka.input-topic}")
    private final String inputTopic;

    @Value("${kafka.output-topic}")
    private final String outputTopic;

    @Override
    public void run(String... args) {
        Topology topology = createTopology();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamProperties);
        kafkaStreams.start();

    }

    @Override
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(value -> value + " is streamed")
                //.peek((key, value) -> log.info("key: {} - value: {}", key, value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }
}
