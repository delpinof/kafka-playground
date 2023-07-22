package com.fherdelpino.kafka.playground.producer.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaProducerCommandLineRunner implements CommandLineRunner {

    @Value("${kafka.topic-name}")
    private String topicName;

    @Autowired
    private Producer<String, String> kafkaProducer;

    @Override
    public void run(String... args) throws Exception {
        for (int i = 0; true; i++) {
            final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, String.format("message %d", i));
            kafkaProducer.send(producerRecord);
            log.info("Message sent successfully!");
            Thread.sleep(1000);
        }
    }
}
