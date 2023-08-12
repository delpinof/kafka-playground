package com.fherdelpino.kafka.playground.streams.service.runnner;

import com.fherdelpino.kafka.playground.common.avro.model.BinanceExchange;
import com.fherdelpino.kafka.playground.streams.service.runner.BinanceExchangeProcessorCommandLineRunner;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.common.serialization.Serde;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class BinanceExchangeProcessorCommandLineRunnerTest {

    private static final String INPUT_TOPIC_NAME = "string-topic-input";
    private static final String OUTPUT_TOPIC_NAME = "string-topic-output";
    private static final String SCHEMA_REGISTRY = "mock://avro-test";

    private static TopologyTestDriver testDriver;
    private static TestInputTopic<String, BinanceExchange> inputTopic;
    private static TestOutputTopic<String, Long> outputTopic;

    @BeforeAll
    public static void configure() {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "string-test-app");
        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamProps.put("schema.registry.url", SCHEMA_REGISTRY);

        SpecificAvroSerializer<BinanceExchange> binanceExchangeSerializer = new SpecificAvroSerializer<>();
        binanceExchangeSerializer.configure(Map.of("schema.registry.url", SCHEMA_REGISTRY), false);

        SpecificAvroDeserializer<BinanceExchange> binanceExchangeDeserializer = new SpecificAvroDeserializer<>();
        binanceExchangeDeserializer.configure(Map.of("schema.registry.url", SCHEMA_REGISTRY), false);

        Serde<BinanceExchange> binanceExchangeSerde = Serdes.serdeFrom(binanceExchangeSerializer, binanceExchangeDeserializer);

        BinanceExchangeProcessorCommandLineRunner sut = new BinanceExchangeProcessorCommandLineRunner(streamProps, INPUT_TOPIC_NAME, OUTPUT_TOPIC_NAME, binanceExchangeSerde);
        Topology topology = sut.createTopology();

        testDriver = new TopologyTestDriver(topology, streamProps);
        inputTopic = testDriver.createInputTopic(INPUT_TOPIC_NAME, Serdes.String().serializer(), binanceExchangeSerializer);
        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC_NAME, Serdes.String().deserializer(), Serdes.Long().deserializer());

    }

    @ParameterizedTest
    @MethodSource("streamData")
    public void test(Map<String, List<BinanceExchange>> inputData, Map<String, Long> expectedOutput) {
        for (String key : inputData.keySet()) {
            for (BinanceExchange exchange : inputData.get(key)) {
                inputTopic.pipeInput(key, exchange);
            }
        }
        testDriver.advanceWallClockTime(Duration.ofMinutes(1));
        Map<String, Long> actualValues = outputTopic.readKeyValuesToMap();
        assertThat(actualValues).isEqualTo(expectedOutput);
    }

    private static Stream<Arguments> streamData() {

        long btcusdtCount = 10;
        long ethusdtCount = 20;
        long xrpusdtCount = 30;

        Map<String, Long> expectedOutput = Map.of("BTCUSDT", btcusdtCount, "ETHUSDT", ethusdtCount, "XRPUSDT", xrpusdtCount);

        BinanceExchange binanceExchange = BinanceExchange.newBuilder()
                .setPrice("10.1")
                .setTimestamp(System.currentTimeMillis()).build();

        Map<String, List<BinanceExchange>> inputData = new HashMap<>();

        for (String key : expectedOutput.keySet()) {
            inputData.put(key, new ArrayList<>());
            for (int i = 0; i < expectedOutput.get(key); i++) {
                inputData.get(key).add(binanceExchange);
            }
        }

        return Stream.of(
                Arguments.of(inputData, expectedOutput)
        );
    }

    @AfterAll
    public static void tearDown() {
        testDriver.close();
    }
}
