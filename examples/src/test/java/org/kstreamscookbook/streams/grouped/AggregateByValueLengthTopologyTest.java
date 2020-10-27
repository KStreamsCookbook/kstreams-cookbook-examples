package org.kstreamscookbook.streams.grouped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class AggregateByValueLengthTopologyTest extends TopologyTestBase {

    @Override
    protected Supplier<Topology> withTopology() {
        return new AggregateByValueLengthTopology(INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @Test
    public void testAggregationByValue() {
        var stringSerializer = new StringSerializer();
        var inputTopic = testDriver.createInputTopic(INPUT_TOPIC, stringSerializer, stringSerializer);

        inputTopic.pipeInput("a", ".");
        inputTopic.pipeInput("b", "..");
        inputTopic.pipeInput("b", "...");
        inputTopic.pipeInput("a", "..");
        inputTopic.pipeInput("c", "...");
        inputTopic.pipeInput("b", ".");

        var integerDeserializer = new IntegerDeserializer();
        var stringDeserializer = new StringDeserializer();
        var outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, integerDeserializer, stringDeserializer);

        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(1,"a"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(2,"b"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(3,"b"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(2,"b,a"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(3,"b,c"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(1,"a,b"));
    }
}