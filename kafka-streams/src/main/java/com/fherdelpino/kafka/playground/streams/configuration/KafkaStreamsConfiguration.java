package com.fherdelpino.kafka.playground.streams.configuration;

import com.fherdelpino.kafka.playground.common.avro.model.BinanceExchange;
import com.fherdelpino.kafka.playground.streams.error.BinanceExchangeDeserializationExceptionHandler;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

@Configuration
public class KafkaStreamsConfiguration {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.application-id}")
    private String applicationId;

    @Value("${kafka.schema-registry}")
    private String schemaRegistry;

    @Bean
    public Properties streamProperties() {
        Properties streamProperties = new Properties();
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamProperties.put("schema.registry.url", schemaRegistry);
        streamProperties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, BinanceExchangeDeserializationExceptionHandler.class);

        return streamProperties;
    }

    @Bean
    public Serde<BinanceExchange> binanceExchangeValueSerde() {
        Serde<BinanceExchange> binanceExchangeSpecificAvroSerde = new SpecificAvroSerde<>();
        Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", schemaRegistry);
        binanceExchangeSpecificAvroSerde.configure(serdeConfig, false);
        return binanceExchangeSpecificAvroSerde;
    }
}
