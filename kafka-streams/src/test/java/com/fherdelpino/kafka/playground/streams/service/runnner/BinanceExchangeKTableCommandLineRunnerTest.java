package com.fherdelpino.kafka.playground.streams.service.runnner;

import com.fherdelpino.kafka.playground.common.avro.model.BinanceExchange;
import com.fherdelpino.kafka.playground.streams.service.runner.BinanceExchangeKTableCommandLineRunner;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class BinanceExchangeKTableCommandLineRunnerTest {
    private static final String INPUT_TOPIC_NAME = "string-topic-input";
    private static final String OUTPUT_TOPIC_NAME = "string-topic-output";
    private static final String SCHEMA_REGISTRY = "mock://avro-test";

    private static TopologyTestDriver testDriver;
    private static TestInputTopic<String, BinanceExchange> inputTopic;
    private static TestOutputTopic<String, BinanceExchange> outputTopic;


    @BeforeAll
    public static void configure() {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "string-test-app");
        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamProps.put("schema.registry.url", SCHEMA_REGISTRY);

        BinanceExchangeKTableCommandLineRunner sut = new BinanceExchangeKTableCommandLineRunner(streamProps, INPUT_TOPIC_NAME, OUTPUT_TOPIC_NAME);
        StreamsBuilder builder = sut.createBuilder();

        SpecificAvroSerializer<BinanceExchange> binanceExchangeSerializer = new SpecificAvroSerializer<>();
        binanceExchangeSerializer.configure(Map.of("schema.registry.url", SCHEMA_REGISTRY), false);

        SpecificAvroDeserializer<BinanceExchange> binanceExchangeDeserializer = new SpecificAvroDeserializer<>();
        binanceExchangeDeserializer.configure(Map.of("schema.registry.url", SCHEMA_REGISTRY), false);

        testDriver = new TopologyTestDriver(builder.build(), streamProps);
        inputTopic = testDriver.createInputTopic(INPUT_TOPIC_NAME, Serdes.String().serializer(), binanceExchangeSerializer);
        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC_NAME, Serdes.String().deserializer(), binanceExchangeDeserializer);
    }

    @ParameterizedTest
    @MethodSource("streamData")
    public void test(Map<String, BinanceExchange> inputData, Map<String, BinanceExchange> expectedOutput) {
        for (Map.Entry<String, BinanceExchange> entry : inputData.entrySet()) {
            inputTopic.pipeInput(entry.getKey(), entry.getValue());
        }
        Map<String, BinanceExchange> actualValues = outputTopic.readKeyValuesToMap();
        assertThat(actualValues).isEqualTo(expectedOutput);
    }

    private static Stream<Arguments> streamData() {
        BinanceExchange binanceExchange = BinanceExchange.newBuilder()
                .setPrice("10.1")
                .setTimestamp(System.currentTimeMillis()).build();

        Map<String, BinanceExchange> inputData = Map.of("BTCUSDT", binanceExchange,
                "ETHUSDT", binanceExchange, "XRPUSDT", binanceExchange);

        Map<String, BinanceExchange> expectedOutput = Map.of("BTCUSDT", binanceExchange);

        return Stream.of(
                Arguments.of(inputData, expectedOutput)
        );
    }

    @AfterAll
    public static void tearDown() {
        testDriver.close();
    }
}
