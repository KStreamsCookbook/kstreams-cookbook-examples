package org.kstreamscookbook;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.Map;
import java.util.Properties;

public abstract class TopologyTestBase {

    protected TopologyTestDriver testDriver;

    @BeforeEach
    void setUp() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        Map<String, String> testProperties = withProperties();
        if (testProperties != null) {
            config.putAll(testProperties);
        }
        testDriver = new TopologyTestDriver(withTopologyBuilder().build(), config);
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    protected abstract TopologyBuilder withTopologyBuilder();

    protected Map<String, String> withProperties() {
        return null;
    }

}
