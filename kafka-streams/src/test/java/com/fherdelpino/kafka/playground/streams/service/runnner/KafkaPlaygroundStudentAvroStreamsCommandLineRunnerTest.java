package com.fherdelpino.kafka.playground.streams.service.runnner;

import com.fherdelpino.kafka.playground.common.avro.model.Student;
import com.fherdelpino.kafka.playground.streams.service.runner.KafkaPlaygroundStudentAvroStreamsCommandLineRunner;
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

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class KafkaPlaygroundStudentAvroStreamsCommandLineRunnerTest {

    private static final String INPUT_TOPIC_NAME = "string-topic-input";
    private static final String OUTPUT_TOPIC_NAME = "string-topic-output";
    private static final String SCHEMA_REGISTRY = "mock://avro-test";

    private static TopologyTestDriver testDriver;
    private static TestInputTopic<String, Student> inputTopic;
    private static TestOutputTopic<String, Student> outputTopic;


    @BeforeAll
    public static void configure() {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "string-test-app");
        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamProps.put("schema.registry.url", SCHEMA_REGISTRY);

        StreamsBuilder builder = new StreamsBuilder();
        KafkaPlaygroundStudentAvroStreamsCommandLineRunner sut = new KafkaPlaygroundStudentAvroStreamsCommandLineRunner(new Properties(), INPUT_TOPIC_NAME, OUTPUT_TOPIC_NAME);
        sut.createBuilder(builder);

        SpecificAvroSerializer<Student> studentSerializer = new SpecificAvroSerializer<>();
        studentSerializer.configure(Map.of("schema.registry.url", SCHEMA_REGISTRY), false);

        SpecificAvroDeserializer<Student> studentDeserializer = new SpecificAvroDeserializer<>();
        studentDeserializer.configure(Map.of("schema.registry.url", SCHEMA_REGISTRY), false);

        testDriver = new TopologyTestDriver(builder.build(), streamProps);
        inputTopic = testDriver.createInputTopic(INPUT_TOPIC_NAME, Serdes.String().serializer(), studentSerializer);
        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC_NAME, Serdes.String().deserializer(), studentDeserializer);
    }

    @ParameterizedTest
    @MethodSource("streamData")
    public void test(List<Student> inputData, List<Student> expectedValues) {
        for (Student record : inputData) {
            inputTopic.pipeInput(record);
        }
        List<Student> actualValues = outputTopic.readValuesToList();
        assertThat(actualValues).isEqualTo(expectedValues);
    }

    private static Stream<Arguments> streamData() {
        List<Student> studentListInput = List.of(
                Student.newBuilder()
                        .setStudentId("1")
                        .setStudentName("Fernando")
                        .setAge(36)
                        .build(),
                Student.newBuilder()
                        .setStudentId("2")
                        .setStudentName("Fernando")
                        .setAge(36)
                        .build(),
                Student.newBuilder()
                        .setStudentId("3")
                        .setStudentName("Fernando")
                        .setAge(36)
                        .build()
        );
        List<Student> studentListOutput = List.of(
                Student.newBuilder()
                        .setStudentId("1")
                        .setStudentName("Alonso")
                        .setAge(36)
                        .build(),
                Student.newBuilder()
                        .setStudentId("2")
                        .setStudentName("Alonso")
                        .setAge(36)
                        .build(),
                Student.newBuilder()
                        .setStudentId("3")
                        .setStudentName("Alonso")
                        .setAge(36)
                        .build()
        );
        return Stream.of(
                Arguments.of(studentListInput, studentListOutput)
        );
    }

    @AfterAll
    public static void tearDown() {
        testDriver.close();
    }
}
