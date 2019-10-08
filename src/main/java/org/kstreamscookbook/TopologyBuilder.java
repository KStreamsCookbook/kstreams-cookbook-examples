package org.kstreamscookbook;

import org.apache.kafka.streams.Topology;

public interface TopologyBuilder {
    Topology build();
}
