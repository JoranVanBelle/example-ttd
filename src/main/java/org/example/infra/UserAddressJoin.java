package org.example.infra;

import AddressesExample.org.example.UserRegistered;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.Topology;
import org.example.AddressRegistered;
import org.example.UserAddressJoined;

import java.util.HashMap;
import java.util.Map;

public class UserAddressJoin {

    public static Topology getTopology(
            String USER_TOPIC,
            String ADDRESS_TOPIC,
            String JOINED_TOPIC
    ) {
        return new Topology();
    }

    public static SpecificAvroSerde<UserRegistered> userSerde(String schema_registry) {
        return null;
    }

    public static SpecificAvroSerde<AddressRegistered> addressSerde(String schema_registry) {
        return null;
    }

    public static SpecificAvroSerde<UserAddressJoined> joinedSerde(String schema_registry) {
        return null;
    }

}