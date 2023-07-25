package com.fherdelpino.kafka.playground.producer.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@ConditionalOnProperty(prefix = "playground", name = "producer-type", havingValue = "string")
public class KafkaPlaygroundStringProducer implements InfiniteProducer {

    @Value("${kafka.string-topic-name}")
    private String stringTopicName;

    @Autowired
    private Producer<String, String> kafkaStringProducer;


    @Override
    public void produce() throws InterruptedException {
        for (int i = 0; true; i++) {
            String message = String.format("message %d", i);
            kafkaStringProducer.send(new ProducerRecord<>(stringTopicName, message));
            log.info("Message sent successfully!");
            Thread.sleep(1000);
        }
    }
}
