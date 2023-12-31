package com.fherdelpino.kafka.playground.streams.service.runner;

import com.fherdelpino.kafka.playground.common.avro.model.BinanceExchange;
import com.fherdelpino.kafka.playground.streams.error.BinanceExchangeUncaughtExceptionHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Properties;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

/**
 * Count the amount of exchanges in a timeframe.
 */
@Slf4j
@RequiredArgsConstructor
@Component
@ConditionalOnProperty(prefix = "playground", name = "stream-type", havingValue = "exchange-windowed")
public class BinanceExchangeStreamWindowedCommandLineRunner implements CommandLineRunner, TopologyBuilder {

    @Autowired
    private final Properties streamProperties;

    @Value("${kafka.input-topic}")
    private final String inputTopic;

    @Autowired
    private final Serde<BinanceExchange> binanceExchangeValueSerde;

    private static final int WINDOW_MINUTES_DURATION = 5;

    @Override
    public void run(String... args) {

        Topology topology = createTopology();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamProperties);
        kafkaStreams.setUncaughtExceptionHandler(new BinanceExchangeUncaughtExceptionHandler());
        kafkaStreams.start();
    }

    @Override
    public Topology createTopology() {

        Duration windowSize = Duration.ofMinutes(WINDOW_MINUTES_DURATION);
        TimeWindows tumblingWindow = TimeWindows.ofSizeWithNoGrace(windowSize);

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), binanceExchangeValueSerde)
                        .withTimestampExtractor(new BinanceExchangeTimestampExtractor()))
                .filter((k, v) -> k.equals("BTCUSDT"))
                .groupByKey()
                .windowedBy(tumblingWindow)
                .count()
                .suppress(untilWindowCloses(unbounded()))
                .toStream()
                //.map((wk, v) -> KeyValue.pair(wk.key(), v))
                .peek((k, v) -> log.info("Output: {} - {}", k, v));

        return builder.build();
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
