package com.fherdelpino.kafka.playground.streams.service.runnner;

import com.fherdelpino.kafka.playground.streams.service.runner.KafkaPlaygroundStringStreamsCommandLineRunner;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

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

        KafkaPlaygroundStringStreamsCommandLineRunner sut = new KafkaPlaygroundStringStreamsCommandLineRunner(streamProps, INPUT_TOPIC_NAME, OUTPUT_TOPIC_NAME);
        Topology topology = sut.createTopology();

        testDriver = new TopologyTestDriver(topology, streamProps);
        inputTopic = testDriver.createInputTopic(INPUT_TOPIC_NAME, Serdes.String().serializer(), Serdes.String().serializer());
        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC_NAME, Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    @ParameterizedTest
    @MethodSource("streamData")
    public void test(List<String> inputData, List<String> expectedValues) {
        for (String record : inputData) {
            inputTopic.pipeInput(record);
        }
        List<String> actualValues = outputTopic.readValuesToList();
        assertThat(actualValues).isEqualTo(expectedValues);
    }

    private static Stream<Arguments> streamData() {
        return Stream.of(
                Arguments.of(List.of("1", "2", "3"), List.of("1 is streamed", "2 is streamed", "3 is streamed"))
        );
    }

    @AfterAll
    public static void tearDown() {
        testDriver.close();
    }
}
