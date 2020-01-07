package org.kstreamscookbook.streams.grouped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.util.function.Supplier;

class AggregateTopologyTest extends TopologyTestBase {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_TOPIC = "output-topic";

    private StringSerializer stringSerializer = new StringSerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private LongDeserializer longDeserializer = new LongDeserializer();

    @Override
    protected Supplier<Topology> withTopology() {
        return new AggregateTopology(INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @Test
    public void testCounts() {
        var factory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "1"));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "b", "2"));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "b", "3"));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "4"));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "5"));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "b", "6"));


        OutputVerifier.compareKeyValue(readNextRecord(), "a", "1");
        OutputVerifier.compareKeyValue(readNextRecord(), "b", "2");
        OutputVerifier.compareKeyValue(readNextRecord(), "b", "2,3");
        OutputVerifier.compareKeyValue(readNextRecord(), "a", "1,4");
        OutputVerifier.compareKeyValue(readNextRecord(), "a", "1,4,5");
        OutputVerifier.compareKeyValue(readNextRecord(), "b", "2,3,6");
    }

    private ProducerRecord<String, String> readNextRecord() {
        return testDriver.readOutput(OUTPUT_TOPIC, stringDeserializer, stringDeserializer);
    }
}