package com.fherdelpino.kafka.playground.streams.service.runner;

import com.fherdelpino.kafka.playground.common.avro.model.BinanceExchange;
import com.fherdelpino.kafka.playground.streams.service.BinanceExchangeProcessorSupplier;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Slf4j
@RequiredArgsConstructor
@Component
@ConditionalOnProperty(prefix = "playground", name = "stream-type", havingValue = "exchange-processor")
public class BinanceExchangeProcessorCommandLineRunner implements CommandLineRunner {

    private final static String storeName = "binance-exchange-countbyticker-store";

    @Autowired
    public final Properties streamProperties;

    @Value("${kafka.input-topic}")
    private final String inputTopic;

    @Value("${kafka.group-by-topic}")
    private final String groupByTopic;

    @Autowired
    private final Serde<BinanceExchange> binanceExchangeValueSerde;

    @Override
    public void run(String... args) {
        final Topology topology = createTopology();

        KafkaStreams streams = new KafkaStreams(topology, streamProperties);
        streams.start();
    }

    public Topology createTopology() {
        String sourceNode = "exchange-source";
        String processorNode = "exchange-count";
        String sinkNode = "exchange-output";
        final Topology topology = new Topology();
        topology.addSource(sourceNode, new StringDeserializer(), binanceExchangeValueSerde.deserializer(), inputTopic);
        topology.addProcessor(processorNode, new BinanceExchangeProcessorSupplier(storeName), sourceNode);
        topology.addSink(sinkNode, groupByTopic, new StringSerializer(), new LongSerializer(), processorNode);
        return topology;
    }
}
