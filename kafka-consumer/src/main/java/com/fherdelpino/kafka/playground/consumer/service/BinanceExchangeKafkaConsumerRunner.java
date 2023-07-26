package com.fherdelpino.kafka.playground.consumer.service;

import com.fherdelpino.kafka.playground.common.avro.model.BinanceExchange;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;

@Slf4j
@Service
@ConditionalOnProperty(prefix = "playground", name = "consumer-type", havingValue = "exchange")
public class BinanceExchangeKafkaConsumerRunner implements CommandLineRunner {

    @Value("${kafka.topic}")
    private String topic;

    @Autowired
    public Consumer<String, BinanceExchange> kafkaBinanceExchangeConsumer;

    @Override
    public void run(String... args) {
        kafkaBinanceExchangeConsumer.subscribe(Collections.singletonList(topic));
        while (true) {
            kafkaBinanceExchangeConsumer.poll(Duration.ofMillis(100))
                    .forEach(record -> log.info("{} - {}", record.key(), convert(record.value())));
        }
    }

    record BinanceExchangeRecord(Date date, BigDecimal price) {
    }

    private BinanceExchangeRecord convert(BinanceExchange exchange) {
        Date date = new Date(exchange.getTimestamp());
        BigDecimal price = new BigDecimal(exchange.getPrice());
        return new BinanceExchangeRecord(date, price);
    }
}
