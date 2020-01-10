package org.kstreamscookbook.tables;

import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class StreamTableJoinTopologyTest extends TopologyTestBase {

    private final static String PRICES_TOPIC = "prices";
    private final static String ORDERS_TOPIC = "orders";
    private final static String PRICED_ORDERS_TOPIC = "priced-orders";

    @Override
    protected Supplier<Topology> withTopology() {
        return new StreamTableJoinTopology(PRICES_TOPIC, ORDERS_TOPIC, PRICED_ORDERS_TOPIC);
    }

    @Test
    public void testOrdersCorrectlyPriced() {
        var stringSerializer = new StringSerializer();
        var doubleSerializer = new DoubleSerializer();

        var pricesTopic = testDriver.createInputTopic(PRICES_TOPIC, stringSerializer, doubleSerializer);
        var ordersTopic = testDriver.createInputTopic(ORDERS_TOPIC, stringSerializer, doubleSerializer);

        var stringDeserializer = new StringDeserializer();
        var doubleDeserializer = new DoubleDeserializer();

        var outputTopic = testDriver.createOutputTopic(PRICED_ORDERS_TOPIC, stringDeserializer, doubleDeserializer);

        // send in prices for two financial instruments - should not emit anything
        pricesTopic.pipeInput("X", 1.2);
        pricesTopic.pipeInput("Y", 1.3);
        assertThat(outputTopic.isEmpty()).isTrue();

        // send in an order for instrumentX
        ordersTopic.pipeInput("X", 10d);

        // check that it is priced
        assertNextKVPair(outputTopic, "X", 10 * 1.2);

        // change the price for instrumentA - should not emit anything
        pricesTopic.pipeInput("X", 1.4);
        assertThat(outputTopic.isEmpty()).isTrue();

        // send in an order for instrumentX
        ordersTopic.pipeInput("X", 15d);
        assertNextKVPair(outputTopic, "X", 15 * 1.4);
    }

    private void assertNextKVPair(TestOutputTopic<String, Double> outputTopic, String k, Double v) {
        var record = outputTopic.readRecord();
        assertThat(record.getKey()).isEqualTo(k);
        assertThat(record.getValue()).isEqualTo(v);
    }
}