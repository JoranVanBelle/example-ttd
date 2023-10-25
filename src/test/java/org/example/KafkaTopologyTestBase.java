package org.example;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;

import java.util.Properties;

public class KafkaTopologyTestBase {

    protected static final String MOCK_SR = "mock://test";
    protected TopologyTestDriver testDriver;

    protected static TopologyTestDriver createTestDriver(Topology topo) {
        Properties testProps = new Properties();
        testProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        testProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        testProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        testProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        testProps.put("schema.registry.url", MOCK_SR);

        return new TopologyTestDriver(topo, testProps);
    }

}
