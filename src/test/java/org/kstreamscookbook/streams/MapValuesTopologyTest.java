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

class MapValuesTopologyTest extends TopologyTestBase {

    @Override
    protected Supplier<Topology> withTopology() {
        return new MapValuesTopology();
    }

    @Test
    void testCopied() {
        var stringSerializer = new StringSerializer();
        var factory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);
        testDriver.pipeInput(factory.create(MapValuesTopology.INPUT_TOPIC, "key", "value"));

        var stringDeserializer = new StringDeserializer();
        OutputVerifier.compareKeyValue(testDriver.readOutput(MapValuesTopology.OUTPUT_TOPIC, stringDeserializer, stringDeserializer), "key", "VALUE");
    }
}