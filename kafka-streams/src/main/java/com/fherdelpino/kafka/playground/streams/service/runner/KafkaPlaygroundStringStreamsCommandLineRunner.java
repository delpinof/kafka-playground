package com.fherdelpino.kafka.playground.streams.service.runner;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "playground", name = "stream-type", havingValue = "string")
public class KafkaPlaygroundStringStreamsCommandLineRunner implements CommandLineRunner {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.input-topic}")
    private String inputTopic;

    @Value("${kafka.output-topic}")
    private String outputTopic;

    @Value("${kafka.application-id}")
    private String applicationId;

    @Override
    public void run(String... args) {
        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(value -> value + " is streamed")
                //.peek((key, value) -> log.info("key: {} - value: {}", key, value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        try (KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps)) {
            kafkaStreams.start();
        }

    }
}