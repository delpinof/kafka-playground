package com.fherdelpino.kafka.playground.streams.service;

import com.fherdelpino.kafka.playground.common.avro.model.BinanceExchange;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "playground", name = "stream-type", havingValue = "exchange-windowed")
public class BinanceExchangeStreamWindowedCommandLineRunner implements CommandLineRunner {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.input-topic}")
    private String inputTopic;

    @Value("${kafka.schema-registry}")
    private String schemaRegistry;

    @Value("${kafka.application-id}")
    private String applicationId;

    @Override
    public void run(String... args) {
        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);

        final Serde<BinanceExchange> valueSpecificAvroSerde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", schemaRegistry);
        valueSpecificAvroSerde.configure(serdeConfig, false);

        Duration windowSize = Duration.ofMinutes(5);
        TimeWindows tumblingWindow = TimeWindows.ofSizeWithNoGrace(windowSize);

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), valueSpecificAvroSerde)
                        .withTimestampExtractor(new BinanceExchangeTimestampExtractor()))
                .filter((k, v) -> k.equals("BTCUSDT"))
                .groupByKey()
                .windowedBy(tumblingWindow)
                .count()
                .suppress(untilWindowCloses(unbounded()))
                .toStream()
                //.map((wk, v) -> KeyValue.pair(wk.key(), v))
                .peek((k, v) -> log.info("Output: {} - {}", k, v));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps);
        kafkaStreams.start();
    }

    /**
     * Extract the timestamp from the record value and use it to window records.
     * A BinanceExchange object will contain the exchange actual timestamp when it was generated.
     */
    static class BinanceExchangeTimestampExtractor implements TimestampExtractor {

        @Override
        public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
            long timestamp = record.timestamp();
            if (record.key() instanceof BinanceExchange binanceExchange) {
                timestamp = binanceExchange.getTimestamp();
            }
            return timestamp;
        }
    }
}
