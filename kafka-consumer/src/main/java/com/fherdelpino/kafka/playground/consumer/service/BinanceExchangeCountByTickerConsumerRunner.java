package com.fherdelpino.kafka.playground.consumer.service;

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
@ConditionalOnProperty(prefix = "playground", name = "consumer-type", havingValue = "exchange-count")
public class BinanceExchangeCountByTickerConsumerRunner implements CommandLineRunner {

    @Value("${kafka.topic}")
    private String topic;

    @Autowired
    public Consumer<String, Long> kafkaBinanceExchangeCountConsumer;

    @Override
    public void run(String... args) {
        kafkaBinanceExchangeCountConsumer.subscribe(Collections.singletonList(topic));
        while (true) {
            kafkaBinanceExchangeCountConsumer.poll(Duration.ofMillis(100))
                    .forEach(record -> log.info("{} - {}", record.key(), record.value()));
        }
    }
}
