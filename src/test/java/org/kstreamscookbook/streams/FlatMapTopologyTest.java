package org.kstreamscookbook.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.util.function.Supplier;

public class FlatMapTopologyTest extends TopologyTestBase {
    @Override
    protected Supplier<Topology> withTopology() {
        return new FlatMapTopology();
    }

    @Test
    public void testMapping() {
        var stringSerializer = new StringSerializer();
        var factory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);
        testDriver.pipeInput(factory.create(MapTopology.INPUT_TOPIC, "key", "This is a  sentence"));

        expectNextKVPair(0, "This");
        expectNextKVPair(1, "is");
        expectNextKVPair(2, "a");
        expectNextKVPair(3, "sentence");
    }

    // TODO refactor this out to make it consistent with other tests
    private void expectNextKVPair(Integer k, String v) {
        StringDeserializer stringDeserializer = new StringDeserializer();
        IntegerDeserializer integerDeserializer = new IntegerDeserializer();

        ProducerRecord<Integer, String> producerRecord = testDriver.readOutput(MapTopology.OUTPUT_TOPIC, integerDeserializer, stringDeserializer);
        OutputVerifier.compareKeyValue(producerRecord, k, v);
    }
}
