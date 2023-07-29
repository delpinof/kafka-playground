package com.fherdelpino.kafka.playground.streams.service;

import com.fherdelpino.kafka.playground.common.avro.model.BinanceExchange;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "playground", name = "stream-type", havingValue = "exchange-ktable")
public class BinanceExchangeKTableCommandLineRunner implements CommandLineRunner {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.input-topic}")
    private String inputTopic;

    @Value("${kafka.output-topic}")
    private String outputTopic;

    @Value("${kafka.schema-registry}")
    private String schemaRegistry;

    @Value("${kafka.application-id}")
    private String applicationId;

    @Override
    public void run(String... args) {
        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsProps.put("schema.registry.url", schemaRegistry);

        final Serde<BinanceExchange> valueSpecificAvroSerde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", schemaRegistry);
        valueSpecificAvroSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, BinanceExchange> binanceExchangeKTable = builder.table(inputTopic,
                Materialized.<String, BinanceExchange, KeyValueStore<Bytes, byte[]>>as("ktable-btc-exchange-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(valueSpecificAvroSerde)
        );
        binanceExchangeKTable.filter((key, value) -> key.equals("BTCUSDT"))
                .toStream()
                .peek((key, value) -> log.info("key: {} - value: {}", key, value))
                .to(outputTopic, Produced.with(Serdes.String(), valueSpecificAvroSerde));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps);
        kafkaStreams.start();
    }

}
