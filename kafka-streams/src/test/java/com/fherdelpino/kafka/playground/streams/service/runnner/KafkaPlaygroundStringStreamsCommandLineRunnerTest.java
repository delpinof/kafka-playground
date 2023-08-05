package com.fherdelpino.kafka.playground.streams.service.runnner;

import com.fherdelpino.kafka.playground.streams.service.runner.KafkaPlaygroundStringStreamsCommandLineRunner;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaPlaygroundStringStreamsCommandLineRunnerTest {

    private static final String INPUT_TOPIC_NAME = "string-topic-input";
    private static final String OUTPUT_TOPIC_NAME = "string-topic-output";
    private static final String SCHEMA_REGISTRY = "mock://aggregation-test";

    private static TopologyTestDriver testDriver;
    private static TestInputTopic<String, String> inputTopic;
    private static TestOutputTopic<String, String> outputTopic;


    @BeforeAll
    public static void configure() {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "string-test-app");
        streamProps.put("schema.registry.url", SCHEMA_REGISTRY);

        StreamsBuilder builder = new StreamsBuilder();
        KafkaPlaygroundStringStreamsCommandLineRunner sut = new KafkaPlaygroundStringStreamsCommandLineRunner(
                streamProps, INPUT_TOPIC_NAME, OUTPUT_TOPIC_NAME);
        sut.createBuilder(builder);

        testDriver = new TopologyTestDriver(builder.build(), streamProps);
        inputTopic = testDriver.createInputTopic(INPUT_TOPIC_NAME, Serdes.String().serializer(), Serdes.String().serializer());
        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC_NAME, Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    @Test
    public void test() {
        List<String> expectedValues = List.of("1 is streamed", "2 is streamed", "3 is streamed");
        for (int i = 1; i <= 3; i++) {
            inputTopic.pipeInput(String.valueOf(i), String.valueOf(i));
        }
        List<String> actualValues = outputTopic.readValuesToList();
        assertEquals(expectedValues, actualValues);
    }

    @AfterAll
    public static void tearDown() {
        testDriver.close();
    }
}
