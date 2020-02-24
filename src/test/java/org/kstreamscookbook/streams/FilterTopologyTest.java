package org.kstreamscookbook.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.util.Date;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class FilterTopologyTest extends TopologyTestBase {

    static final String INPUT_TOPIC = "input-topic";
    static final String OUTPUT_TOPIC = "output-topic";

    @Override
    protected Supplier<Topology> withTopology() {
        return new FilterTopology(INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @Test
    void testFiltered() {
        var integerSerializer = new IntegerSerializer();
        var inputTopic = testDriver.createInputTopic(INPUT_TOPIC, integerSerializer, integerSerializer);

        // send in some purchases
        // NOTE: we have to send a timestamp when sending Integers or Longs as the both key and value to distinguish between
        // factory.create(K, V) and factory.create(V, timestampMs:long)
        // NOTE: Kafka has 3 notions of time - the test method below allows us to simulate Event time
        inputTopic.pipeInput(1001, 1000, new Date().getTime());
        inputTopic.pipeInput(1001, 500, new Date().getTime());
        inputTopic.pipeInput(1002, 1200, new Date().getTime());

        var integerDeserializer = new IntegerDeserializer();
        var outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, integerDeserializer,integerDeserializer);

        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(1001,1000));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(1002,1200));
    }

}