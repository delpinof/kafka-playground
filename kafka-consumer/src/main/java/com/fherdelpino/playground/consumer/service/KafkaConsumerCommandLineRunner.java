package com.fherdelpino.playground.consumer.service;

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

    @Value("${kafka.topic-name}")
    private String topicName;

    @Autowired
    private Consumer<String, String> kafkaConsumer;

    @Override
    public void run(String... args) {
        kafkaConsumer.subscribe(Collections.singletonList(topicName));
        while (true) {
            kafkaConsumer.poll(Duration.ofMillis(100))
                    .forEach(record -> log.info("{}", record.value()));
        }
    }
}
