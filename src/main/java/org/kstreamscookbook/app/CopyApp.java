package org.kstreamscookbook.app;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class CopyApp {
    private static Logger LOG = LoggerFactory.getLogger(CopyApp.class);

    public static void main(String[] args) {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-copy");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        var topology = new CopyTopology().get();
        var kafkaStreams = new KafkaStreams(topology, props);

        var latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down app now");
            kafkaStreams.close();
            latch.countDown();
        }));

        try {
            kafkaStreams.start();
            latch.await();
        } catch (Throwable t) {
            System.exit(1);
        }
        System.exit(0);

    }
}
