package org.kstreamscookbook.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class FilterNotTopologyTest extends TopologyTestBase {

    static final String INPUT_TOPIC = "input-topic";
    static final String OUTPUT_TOPIC = "output-topic";

    @Override
    protected Supplier<Topology> withTopology() {
        return new FilterNotTopology(INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @Test
    void testFiltered() {
        var integerSerializer = new IntegerSerializer();
        var stringSerializer = new StringSerializer();

        TestInputTopic<Integer, String> inputTopic = testDriver.createInputTopic(INPUT_TOPIC, integerSerializer, stringSerializer);

        // send in some country codes associated with accounts
        inputTopic.pipeInput(1001, "UK");
        inputTopic.pipeInput(1002, "SE");
        inputTopic.pipeInput(1003, "DE");

        var integerDeserializer = new IntegerDeserializer();
        var stringDeserializer = new StringDeserializer();

        TestOutputTopic<Integer, String> outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, integerDeserializer, stringDeserializer);

        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(1002,"SE"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(1003,"DE"));
        assertThat(outputTopic.isEmpty()).isTrue();
    }
}