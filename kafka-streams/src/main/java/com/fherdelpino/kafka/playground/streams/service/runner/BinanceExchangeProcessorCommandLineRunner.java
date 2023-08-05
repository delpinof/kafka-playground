package com.fherdelpino.kafka.playground.streams.service.runner;

import com.fherdelpino.kafka.playground.common.avro.model.BinanceExchange;
import com.fherdelpino.kafka.playground.streams.service.BinanceExchangeProcessorSupplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "playground", name = "stream-type", havingValue = "exchange-processor")
public class BinanceExchangeProcessorCommandLineRunner implements CommandLineRunner {

    final static String storeName = "binance-exchange-countbyticker-store";

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.input-topic}")
    private String inputTopic;

    @Value("${kafka.schema-registry}")
    private String schemaRegistry;

    @Autowired
    private Serde<BinanceExchange> binanceExchangeValueSerde;

    @Override
    public void run(String... args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "exchange-topology-processor");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put("schema.registry.url", schemaRegistry);

        final Topology topology = new Topology();
        String sourceNode = "exchange-source";
        String processorNode = "exchange-count";
        String sinkNode = "exchange-output";
        String groupByTopic = "exchange-groupby-ticker-count";
        topology.addSource(sourceNode, new StringDeserializer(), binanceExchangeValueSerde.deserializer(), inputTopic);
        topology.addProcessor(processorNode, new BinanceExchangeProcessorSupplier(storeName), sourceNode);
        topology.addSink(sinkNode, groupByTopic, new StringSerializer(), new LongSerializer(), processorNode);
        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();
    }
}
