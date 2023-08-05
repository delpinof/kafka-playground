package com.fherdelpino.kafka.playground.streams.service;

import com.fherdelpino.kafka.playground.common.avro.model.BinanceExchange;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Set;

@Slf4j
public class BinanceExchangeProcessorSupplier implements ProcessorSupplier<String, BinanceExchange, String, Long> {
    final String storeName;
    final StoreBuilder<KeyValueStore<String, Long>> binanceExchangeStoreBuilder;

    public BinanceExchangeProcessorSupplier(String storeName) {
        this.storeName = storeName;
        this.binanceExchangeStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(storeName),
                Serdes.String(),
                Serdes.Long());
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return Collections.singleton(binanceExchangeStoreBuilder);
    }

    @Override
    public Processor<String, BinanceExchange, String, Long> get() {
        return new BinanceExchangeCountByTickerProcessor(storeName);
    }

}
