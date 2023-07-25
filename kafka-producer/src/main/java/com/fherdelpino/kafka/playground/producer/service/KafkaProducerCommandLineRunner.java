package com.fherdelpino.kafka.playground.producer.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaProducerCommandLineRunner implements CommandLineRunner {

    @Autowired
    private InfiniteProducer infiniteProducer;

    @Override
    public void run(String... args) throws Exception {
        infiniteProducer.produce();
    }

}
