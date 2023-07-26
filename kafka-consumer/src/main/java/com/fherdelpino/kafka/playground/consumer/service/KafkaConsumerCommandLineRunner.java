package com.fherdelpino.kafka.playground.consumer.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaConsumerCommandLineRunner implements CommandLineRunner {

    @Autowired
    private KafkaPlaygroundConsumer kafkaPlaygroundConsumer;

    @Override
    public void run(String... args) {
        kafkaPlaygroundConsumer.consume();
    }

}
