package org.kstreamscookbook.streams.grouped;

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

class AggregateByValueLengthTopologyTest extends TopologyTestBase {

    private StringSerializer stringSerializer = new StringSerializer();
    private IntegerDeserializer integerDeserializer = new IntegerDeserializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();

    @Override
    protected Supplier<Topology> withTopology() {
        return new AggregateByValueLengthTopology(INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @Test
    public void testAggregation() {
        var factory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "."));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "b", ".."));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "b", "..."));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", ".."));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "c", "..."));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "b", "."));


        OutputVerifier.compareKeyValue(readNextRecord(), 1, "a");
        OutputVerifier.compareKeyValue(readNextRecord(), 2, "b");
        OutputVerifier.compareKeyValue(readNextRecord(), 3, "b");
        OutputVerifier.compareKeyValue(readNextRecord(), 2, "b,a");
        OutputVerifier.compareKeyValue(readNextRecord(), 3, "b,c");
        OutputVerifier.compareKeyValue(readNextRecord(), 1, "a,b");
    }

    // TODO consider refactoring out to make it consistent
    private ProducerRecord<Integer, String> readNextRecord() {
        return testDriver.readOutput(OUTPUT_TOPIC, integerDeserializer, stringDeserializer);
    }
}