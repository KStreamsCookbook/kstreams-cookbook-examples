package org.kstreamscookbook.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PeekTopologyTest extends TopologyTestBase {

    private List<String> outputList = new ArrayList<>();

    @Override
    protected Supplier<Topology> withTopology() {
        return new PeekTopology(INPUT_TOPIC, OUTPUT_TOPIC).withOutputList(outputList);
    }

    @Test
    void testFiltered() {
        var integerSerializer = new IntegerSerializer();
        var inputTopic = testDriver.createInputTopic(INPUT_TOPIC, integerSerializer, integerSerializer);

        // send in some purchases
        // NOTE: we have to send a timestamp when sending Integers or Longs as both key and value to distinguish between
        // factory.create(K, V) and factory.create(V, timestampMs:long)
        // NOTE: Kafka has 3 notions of time - the test method below allows us to simulate Event time
        inputTopic.pipeInput(1001, 1000, new Date().getTime());
        inputTopic.pipeInput(1001, 500, new Date().getTime());
        inputTopic.pipeInput(1002, 1200, new Date().getTime());

        assertTrue(outputList.contains("1001:1000"));
        assertFalse(outputList.contains("1001:500"));
        assertTrue(outputList.contains("1002:1200"));

        var integerDeserializer = new IntegerDeserializer();
        var outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, integerDeserializer, integerDeserializer);

        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(1001, 1000));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(1002, 1200));
    }

}