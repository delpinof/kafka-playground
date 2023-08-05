package com.fherdelpino.kafka.playground.streams.service.runnner;

import com.fherdelpino.kafka.playground.streams.service.runner.KafkaPlaygroundStringStreamsCommandLineRunner;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaPlaygroundStringStreamsCommandLineRunnerTest {


    private static KafkaPlaygroundStringStreamsCommandLineRunner sut;

    private static final String INPUT_TOPIC_NAME = "string-topic-input";

    private static final String OUTPUT_TOPIC_NAME = "string-topic-output";

    private static final String SCHEMA_REGISTRY = "mock://aggregation-test";

    private static final Properties STREAM_PROPERTIES = new Properties();

    private final static StreamsBuilder builder = new StreamsBuilder();

    @BeforeAll
    public static void init() {
        STREAM_PROPERTIES.put(StreamsConfig.APPLICATION_ID_CONFIG, "string-test");
        STREAM_PROPERTIES.put("schema.registry.url", SCHEMA_REGISTRY);
        sut = new KafkaPlaygroundStringStreamsCommandLineRunner(STREAM_PROPERTIES, INPUT_TOPIC_NAME, OUTPUT_TOPIC_NAME);
    }

    @Test
    public void test() {
        List<String> expectedValues = List.of("1 is streamed", "2 is streamed", "3 is streamed");
        sut.createBuilder(builder);
        try (final TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), STREAM_PROPERTIES)) {
            final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(INPUT_TOPIC_NAME, Serdes.String().serializer(), Serdes.String().serializer());
            final TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC_NAME, Serdes.String().deserializer(), Serdes.String().deserializer());
            for (int i = 1; i <= 3; i++) {
                inputTopic.pipeInput(String.valueOf(i), String.valueOf(i));
            }
            List<String> actualValues = outputTopic.readValuesToList();
            assertEquals(expectedValues, actualValues);
        }
    }
}
