package com.fherdelpino.kafka.playground.streams.configuration;

import com.fherdelpino.kafka.playground.common.avro.model.BinanceExchange;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.Map;

@Configuration
public class KafkaStreamsConfiguration {

    @Value("${kafka.schema-registry}")
    private String schemaRegistry;

    @Bean
    public Serde<BinanceExchange> binanceExchangeValueSerde() {
        Serde<BinanceExchange> binanceExchangeSpecificAvroSerde = new SpecificAvroSerde<>();
        Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", schemaRegistry);
        binanceExchangeSpecificAvroSerde.configure(serdeConfig, false);
        return binanceExchangeSpecificAvroSerde;
    }
}
