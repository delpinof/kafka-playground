package com.fherdelpino.kafka.playground.consumer.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;

@Slf4j
@Service
@ConditionalOnProperty(prefix = "playground", name = "consumer-type", havingValue = "string")
public class KafkaPlaygroundStringConsumer implements KafkaPlaygroundConsumer {

    @Value("${kafka.string-topic-name}")
    private String stringTopicName;

    @Autowired
    private Consumer<String, String> kafkaStringConsumer;

    @Override
    public void consume() {
        kafkaStringConsumer.subscribe(Collections.singletonList(stringTopicName));
        while (true) {
            kafkaStringConsumer.poll(Duration.ofMillis(100))
                    .forEach(record -> log.info("{}", record.value()));
        }
    }
}
