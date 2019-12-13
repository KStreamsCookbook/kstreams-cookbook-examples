package org.kstreamscookbook.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyBuilder;
import org.kstreamscookbook.TopologyTestBase;

public class FlatMapTopologyTest extends TopologyTestBase {
    @Override
    protected TopologyBuilder withTopologyBuilder() {
        return new FlatMapTopology();
    }

    @Test
    public void testMapping() {
        StringSerializer stringSerializer = new StringSerializer();
        ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);
        testDriver.pipeInput(factory.create(MapTopology.INPUT_TOPIC, "key", "This is a  sentence"));

        expectNextKVPair(0, "This");
        expectNextKVPair(1, "is");
        expectNextKVPair(2, "a");
        expectNextKVPair(3, "sentence");
    }

    private void expectNextKVPair(Integer k, String v) {
        StringDeserializer stringDeserializer = new StringDeserializer();
        IntegerDeserializer integerDeserializer = new IntegerDeserializer();

        ProducerRecord<Integer, String> producerRecord = testDriver.readOutput(MapTopology.OUTPUT_TOPIC, integerDeserializer, stringDeserializer);
        OutputVerifier.compareKeyValue(producerRecord, k, v);
    }
}
