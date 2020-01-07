package org.kstreamscookbook.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.util.function.Supplier;

class FilterNotTopologyTest extends TopologyTestBase {

    @Override
    protected Supplier<Topology> withTopology() {
        return new FilterNotTopology();
    }

    @Test
    void testFiltered() {
        var integerSerializer = new IntegerSerializer();
        var stringSerializer = new StringSerializer();

        var factory =
                new ConsumerRecordFactory<>(FilterNotTopology.INPUT_TOPIC, integerSerializer, stringSerializer);

        // send in some country codes associated with accounts
        testDriver.pipeInput(factory.create(1001, "UK"));
        testDriver.pipeInput(factory.create(1002, "SE"));
        testDriver.pipeInput(factory.create(1003, "DE"));

        var integerDeserializer = new IntegerDeserializer();
        var stringDeserializer = new StringDeserializer();

        OutputVerifier.compareKeyValue(testDriver.readOutput(FilterNotTopology.OUTPUT_TOPIC, integerDeserializer, stringDeserializer), 1002, "SE");
        OutputVerifier.compareKeyValue(testDriver.readOutput(FilterNotTopology.OUTPUT_TOPIC, integerDeserializer, stringDeserializer), 1003, "DE");
    }
}