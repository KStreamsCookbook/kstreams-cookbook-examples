package org.kstreamscookbook.streams.grouped;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class CountByValueLengthTopologyTest extends TopologyTestBase {

    @Override
    protected Supplier<Topology> withTopology() {
        return new CountByValueLengthTopology(INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @Test
    public void testCountValueLength() {
        var stringSerializer = new StringSerializer();
        var inputTopic = testDriver.createInputTopic(INPUT_TOPIC, stringSerializer, stringSerializer);

        inputTopic.pipeInput("ignored_a", ".");
        inputTopic.pipeInput("ignored_b", "..");
        inputTopic.pipeInput("ignored_c", "...");
        inputTopic.pipeInput("ignored_d", "..");
        inputTopic.pipeInput("ignored_e", "...");
        inputTopic.pipeInput("ignored_f", ".");
        inputTopic.pipeInput("ignored_g", ".....");

        var integerDeserializer = new IntegerDeserializer();
        var longDeserializer = new LongDeserializer();
        var outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, integerDeserializer, longDeserializer);

        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(1, 1L));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(2, 1L));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(3, 1L));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(2, 2L));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(3, 2L));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(1, 2L));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(5, 1L));
    }
}