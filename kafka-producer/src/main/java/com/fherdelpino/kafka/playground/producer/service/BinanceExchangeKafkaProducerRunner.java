package com.fherdelpino.kafka.playground.producer.service;

import com.binance.api.client.BinanceApiClientFactory;
import com.binance.api.client.BinanceApiWebSocketClient;
import com.binance.api.client.domain.event.AggTradeEvent;
import com.fherdelpino.kafka.playground.common.avro.model.BinanceExchange;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
@ConditionalOnProperty(prefix = "playground", name = "producer-type", havingValue = "exchange")
public class BinanceExchangeKafkaProducerRunner implements CommandLineRunner {

    @Value("#{'${binance.exchange.tickers}'.split(',')}")
    private List<String> tickers;

    @Value("${kafka.topic}")
    private String topic;

    @Autowired
    private Producer<String, SpecificRecord> kafkaAvroProducer;

    @Override
    public void run(String... args) {
        BinanceApiWebSocketClient binanceClient = BinanceApiClientFactory.newInstance().newWebSocketClient();
        tickers.stream()
                .map(String::trim)
                .forEach(ticker -> {
                    log.info("Starting client for ticker {}", ticker);
                    binanceClient.onAggTradeEvent(ticker.toLowerCase(), this::produceEvent);
                });
    }

    private void produceEvent(AggTradeEvent event) {
        String key = event.getSymbol();
        BinanceExchange value = BinanceExchange.newBuilder()
                .setPrice(event.getPrice())
                .setTimestamp(event.getEventTime())
                .build();
        kafkaAvroProducer.send(new ProducerRecord<>(topic, key, value));
        log.info("Ticker {} price {}", event.getSymbol(), event.getPrice());
    }
}
