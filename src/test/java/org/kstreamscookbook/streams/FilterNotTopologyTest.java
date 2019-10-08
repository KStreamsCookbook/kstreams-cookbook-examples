package org.kstreamscookbook.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyBuilder;
import org.kstreamscookbook.TopologyTestBase;

class FilterNotTopologyTest extends TopologyTestBase {

    @Override
    protected TopologyBuilder withTopologyBuilder() {
        return new FilterNotTopology();
    }

    @Test
    void testFiltered() {
        IntegerSerializer integerSerializer = new IntegerSerializer();
        StringSerializer stringSerializer = new StringSerializer();

        ConsumerRecordFactory<Integer, String> factory =
                new ConsumerRecordFactory<>(FilterNotTopology.INPUT_TOPIC, integerSerializer, stringSerializer);

        // send in some country codes associated with accounts
        testDriver.pipeInput(factory.create(1001, "UK"));
        testDriver.pipeInput(factory.create(1002, "SE"));
        testDriver.pipeInput(factory.create(1003, "DE"));

        IntegerDeserializer integerDeserializer = new IntegerDeserializer();
        StringDeserializer stringDeserializer = new StringDeserializer();
        {
            ProducerRecord<Integer, String> producerRecord = testDriver.readOutput(FilterNotTopology.OUTPUT_TOPIC, integerDeserializer, stringDeserializer);
            OutputVerifier.compareKeyValue(producerRecord, 1002, "SE");
        }
        {
            ProducerRecord<Integer, String> producerRecord = testDriver.readOutput(FilterNotTopology.OUTPUT_TOPIC, integerDeserializer, stringDeserializer);
            OutputVerifier.compareKeyValue(producerRecord, 1003, "DE");
        }

    }
}