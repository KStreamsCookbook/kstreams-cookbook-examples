package org.kstreamscookbook.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.util.function.Supplier;

class MapTopologyTest extends TopologyTestBase {

    @Override
    protected Supplier<Topology> withTopology() {
        return new MapTopology();
    }

    @Test
    void testCopied() {
        var stringSerializer = new StringSerializer();
        var factory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);
        testDriver.pipeInput(factory.create(MapTopology.INPUT_TOPIC, "key", "value"));

        var stringDeserializer = new StringDeserializer();
        OutputVerifier.compareKeyValue(testDriver.readOutput(MapTopology.OUTPUT_TOPIC, stringDeserializer, stringDeserializer), "k", "VALUE");
    }
}