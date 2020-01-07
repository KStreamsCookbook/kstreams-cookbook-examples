package org.kstreamscookbook.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.util.Date;
import java.util.function.Supplier;

class FilterTopologyTest extends TopologyTestBase {

    @Override
    protected Supplier<Topology> withTopology() {
        return new FilterTopology();
    }

    @Test
    void testFiltered() {
        var integerSerializer = new IntegerSerializer();
        var factory = new ConsumerRecordFactory<>(FilterTopology.INPUT_TOPIC, integerSerializer, integerSerializer);

        // send in some purchases
        // NOTE: we have to send a timestamp when sending Integers or Longs as the both key and value to distinguish between
        // factory.create(K, V) and factory.create(V, timestampMs:long)
        // NOTE: Kafka has 3 notions of time - the test method below allows us to simulate Event time
        testDriver.pipeInput(factory.create(1001, 1000, new Date().getTime()));
        testDriver.pipeInput(factory.create(1001, 500, new Date().getTime()));
        testDriver.pipeInput(factory.create(1002, 1200, new Date().getTime()));

        var integerDeserializer = new IntegerDeserializer();
        OutputVerifier.compareKeyValue(testDriver.readOutput(FilterTopology.OUTPUT_TOPIC, integerDeserializer, integerDeserializer), 1001, 1000);
        OutputVerifier.compareKeyValue(testDriver.readOutput(FilterTopology.OUTPUT_TOPIC, integerDeserializer, integerDeserializer), 1002, 1200);
    }

}