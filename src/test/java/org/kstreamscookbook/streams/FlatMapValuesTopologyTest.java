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

public class FlatMapValuesTopologyTest extends TopologyTestBase {
    @Override
    protected Supplier<Topology> withTopologySupplier() {
        return new FlatMapValuesTopology();
    }

    @Test
    public void testMapping() {
        StringSerializer stringSerializer = new StringSerializer();
        ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);
        testDriver.pipeInput(factory.create(MapTopology.INPUT_TOPIC, "key", "This is a  sentence"));

        expectNextKVPair("key", "This");
        expectNextKVPair("key", "is");
        expectNextKVPair("key", "a");
        expectNextKVPair("key", "sentence");
    }

    private void expectNextKVPair(String k, String v) {
        StringDeserializer stringDeserializer = new StringDeserializer();
        ProducerRecord<String, String> producerRecord = testDriver.readOutput(MapTopology.OUTPUT_TOPIC, stringDeserializer, stringDeserializer);
        OutputVerifier.compareKeyValue(producerRecord, k, v);
    }
}
