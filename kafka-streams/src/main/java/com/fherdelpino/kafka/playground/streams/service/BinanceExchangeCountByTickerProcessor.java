package com.fherdelpino.kafka.playground.streams.service;

import com.fherdelpino.kafka.playground.common.avro.model.BinanceExchange;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.Optional;

@Slf4j
public class BinanceExchangeCountByTickerProcessor implements Processor<String, BinanceExchange, String, Long> {

    private ProcessorContext<String, Long> context;
    private KeyValueStore<String, Long> store;

    private final String storeName;

    public BinanceExchangeCountByTickerProcessor(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<String, Long> context) {
        this.context = context;
        store = context.getStateStore(storeName);
        this.context.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, this::forwardAll);
    }

    @Override
    public void process(Record<String, BinanceExchange> record) {
        Optional<Long> currentValue = Optional.ofNullable(store.get(record.key()));
        store.put(record.key(), currentValue.orElse(0L) + 1L);
    }

    private void forwardAll(final long timestamp) {
        try (KeyValueIterator<String, Long> iterator = store.all()) {
            while (iterator.hasNext()) {
                final KeyValue<String, Long> nextKV = iterator.next();
                final Record<String, Long> exchangeCountByTicker = new Record<>(nextKV.key, nextKV.value, timestamp);
                context.forward(exchangeCountByTicker);
                log.info("Punctuation forwarded record - key {} value {}", exchangeCountByTicker.key(), exchangeCountByTicker.value());
            }
        }
    }
}