package org.kstreamscookbook.streams.grouped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyBuilder;
import org.kstreamscookbook.TopologyTestBase;

class CountByValueLengthTopologyTest extends TopologyTestBase {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_TOPIC = "output-topic";

    private StringSerializer stringSerializer = new StringSerializer();
    private IntegerDeserializer integerDeserializer = new IntegerDeserializer();
    private LongDeserializer longDeserializer = new LongDeserializer();

    @Override
    protected TopologyBuilder withTopologyBuilder() {
        return new CountByValueLengthTopology(INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @Test
    public void testCounts() {
        ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "."));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "b", ".."));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "b", "..."));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", ".."));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "..."));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "b", "."));


        OutputVerifier.compareKeyValue(readNextRecord(), 1, 1l);
        OutputVerifier.compareKeyValue(readNextRecord(), 2, 1l);
        OutputVerifier.compareKeyValue(readNextRecord(), 3, 1l);
        OutputVerifier.compareKeyValue(readNextRecord(), 2, 2l);
        OutputVerifier.compareKeyValue(readNextRecord(), 3, 2l);
        OutputVerifier.compareKeyValue(readNextRecord(), 1, 2l);
    }

    private ProducerRecord<Integer, Long> readNextRecord() {
        return testDriver.readOutput(OUTPUT_TOPIC, integerDeserializer, longDeserializer);
    }
}