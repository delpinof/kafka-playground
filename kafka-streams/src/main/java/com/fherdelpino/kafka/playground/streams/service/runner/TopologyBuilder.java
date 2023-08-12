package com.fherdelpino.kafka.playground.streams.service.runner;

import org.apache.kafka.streams.Topology;

public interface TopologyBuilder {

    Topology createTopology();
}
