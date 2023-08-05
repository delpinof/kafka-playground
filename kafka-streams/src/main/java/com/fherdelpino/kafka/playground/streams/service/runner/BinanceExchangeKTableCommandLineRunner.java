package com.fherdelpino.kafka.playground.streams.service.runner;

import com.fherdelpino.kafka.playground.common.avro.model.BinanceExchange;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "playground", name = "stream-type", havingValue = "exchange-ktable")
public class BinanceExchangeKTableCommandLineRunner implements CommandLineRunner {

    @Autowired
    private Properties streamProperties;

    @Value("${kafka.input-topic}")
    private String inputTopic;

    @Value("${kafka.output-topic}")
    private String outputTopic;

    @Autowired
    private Serde<BinanceExchange> binanceExchangeValueSerde;

    @Override
    public void run(String... args) {

        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, BinanceExchange> binanceExchangeKTable = builder.table(inputTopic,
                Materialized.<String, BinanceExchange, KeyValueStore<Bytes, byte[]>>as("ktable-btc-exchange-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(binanceExchangeValueSerde)
        );
        binanceExchangeKTable.filter((key, value) -> key.equals("BTCUSDT"))
                .toStream()
                .peek((key, value) -> log.info("key: {} - value: {}", key, value))
                .to(outputTopic, Produced.with(Serdes.String(), binanceExchangeValueSerde));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamProperties);
        kafkaStreams.start();
    }

}
