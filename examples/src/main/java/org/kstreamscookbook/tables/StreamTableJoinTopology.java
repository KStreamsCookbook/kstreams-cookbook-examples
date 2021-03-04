package org.kstreamscookbook.tables;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.function.Supplier;

/**
 * Consumes from two topics - one as a prices table and one for orders.
 * The price is keyed by a financial instrument, the value is the current price.
 * The order is keyed by the instrument being purchased, and the value is the volume.
 * Joins the order to the price for the instrument by multiplying the two values.
 */
public class StreamTableJoinTopology implements Supplier<Topology> {

    private final String pricesTopic;
    private final String ordersTopic;
    private final String pricedOrdersTopic;

    public StreamTableJoinTopology(String priceTableTopic, String streamTopic, String outputTopic) {
        this.pricesTopic = priceTableTopic;
        this.ordersTopic = streamTopic;
        this.pricedOrdersTopic = outputTopic;
    }

    @Override
    public Topology get() {
        var stringSerde = Serdes.String();
        var doubleSerde = Serdes.Double();
        var builder = new StreamsBuilder();

        KTable<String, Double> pricesTable = builder.table(pricesTopic, Consumed.with(stringSerde, doubleSerde));

        builder.stream(ordersTopic, Consumed.with(stringSerde, doubleSerde))
                .join(pricesTable, (orderVolume, price) -> orderVolume * price)
                .to(pricedOrdersTopic, Produced.with(stringSerde, doubleSerde));

        return builder.build();
    }
}
