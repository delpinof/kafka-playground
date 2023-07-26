package com.fherdelpino.kafka.playground.consumer.service;

import com.fherdelpino.kafka.playground.common.avro.model.Student;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;

@Slf4j
@Service
@ConditionalOnProperty(prefix = "playground", name = "consumer-type", havingValue = "student")
public class KafkaPlaygroundStudentConsumerRunner implements CommandLineRunner {

    @Value("${kafka.topic}")
    private String topic;

    @Autowired
    private Consumer<String, Student> kafkaStudentAvroConsumer;

    @Override
    public void run(String... args) {
        kafkaStudentAvroConsumer.subscribe(Collections.singletonList(topic));
        while(true) {
            kafkaStudentAvroConsumer.poll(Duration.ofMillis(100))
                    .forEach(record -> log.info("{} - {}", record.key(), record.value()));
        }
    }
}
