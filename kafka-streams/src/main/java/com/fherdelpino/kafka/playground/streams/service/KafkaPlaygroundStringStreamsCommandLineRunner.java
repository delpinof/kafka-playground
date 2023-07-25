package com.fherdelpino.kafka.playground.streams.service;

import com.fherdelpino.kafka.playground.common.avro.model.Student;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Slf4j
@Component
public class KafkaPlaygroundStringStreamsCommandLineRunner implements CommandLineRunner {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.input-topic}")
    private String inputTopic;

    @Value("${kafka.output-topic}")
    private String outputTopic;

    @Value("${kafka.schema-registry}")
    private String schemaRegistry;

    @Value("${kafka.application-id}")
    private String applicationId;

    @Override
    public void run(String... args) {
        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsProps.put("schema.registry.url", schemaRegistry);
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Student> stream = builder.stream(inputTopic);
        stream.mapValues(student -> Student.newBuilder(student).setStudentName("Alonso").build())
                //.peek((key, value) -> log.info("key: {} - value: {}", key, value))
                .to(outputTopic);

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps);
        kafkaStreams.start();
    }
}
