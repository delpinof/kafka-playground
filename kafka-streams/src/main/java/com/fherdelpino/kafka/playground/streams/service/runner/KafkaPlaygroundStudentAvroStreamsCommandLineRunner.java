package com.fherdelpino.kafka.playground.streams.service.runner;

import com.fherdelpino.kafka.playground.common.avro.model.Student;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Slf4j
@RequiredArgsConstructor
@Component
@ConditionalOnProperty(prefix = "playground", name = "stream-type", havingValue = "student")
public class KafkaPlaygroundStudentAvroStreamsCommandLineRunner implements CommandLineRunner {

    @Autowired
    private final Properties streamProperties;

    @Value("${kafka.input-topic}")
    private final String inputTopic;

    @Value("${kafka.output-topic}")
    private final String outputTopic;

    @Override
    public void run(String... args) {

        streamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        StreamsBuilder builder = new StreamsBuilder();
        createBuilder(builder);

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamProperties);
        kafkaStreams.start();
    }

    public void createBuilder(StreamsBuilder builder) {
        KStream<String, Student> stream = builder.stream(inputTopic);
        stream.mapValues(student -> Student.newBuilder(student).setStudentName("Alonso").build())
                //.peek((key, value) -> log.info("key: {} - value: {}", key, value))
                .to(outputTopic);
    }
}
