package com.fherdelpino.kafka.playground.consumer.service;

import com.fherdelpino.kafka.playground.common.avro.model.Student;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;

@Component
@Slf4j
public class KafkaConsumerCommandLineRunner implements CommandLineRunner {

    @Value("${kafka.string-topic-name}")
    private String stringTopicName;

    @Value("${kafka.student-avro-topic-name}")
    private String studentAvroTopicName;

    @Autowired
    private Consumer<String, String> kafkaStringConsumer;

    @Autowired
    private Consumer<String, Student> kafkaStudentAvroConsumer;

    @Override
    public void run(String... args) {
        pollAvroStudentTopic();
    }

    private void pollStringTopic() {
        kafkaStringConsumer.subscribe(Collections.singletonList(stringTopicName));
        while (true) {
            kafkaStringConsumer.poll(Duration.ofMillis(100))
                    .forEach(record -> log.info("{}", record.value()));
        }
    }

    private void pollAvroStudentTopic() {
        kafkaStudentAvroConsumer.subscribe(Collections.singletonList(studentAvroTopicName));
        while(true) {
            kafkaStudentAvroConsumer.poll(Duration.ofMillis(100))
                    .forEach(record -> log.info("{}", record.value()));
        }
    }
}
