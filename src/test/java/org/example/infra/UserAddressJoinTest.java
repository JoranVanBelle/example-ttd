package org.example.infra;

import AddressesExample.org.example.UserRegistered;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.AddressRegistered;
import org.example.KafkaTopologyTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UserAddressJoinTest extends KafkaTopologyTestBase {

    private static final String USER_TOPIC = "user";
    private static final String ADDRESS_TOPIC = "address";
    private static final String JOINED_TOPIC = "joined";

    @BeforeEach
    void beforeEach() {
        this.testDriver = createTestDriver(
                UserAddressJoin.getTopology(
                        USER_TOPIC,
                        ADDRESS_TOPIC,
                        JOINED_TOPIC)
        );
    }

    @AfterEach
    void afterEach() {
        this.testDriver.close();
    }

    @Test
    public void usersAndAddressesCanBeJoined_noErrors() {

        var userTopic = testDriver.createInputTopic(USER_TOPIC, new StringSerializer(), UserAddressJoin.userSerde(MOCK_SR).serializer());
        var addressTopic = testDriver.createInputTopic(ADDRESS_TOPIC, new StringSerializer(), UserAddressJoin.addressSerde(MOCK_SR).serializer());

        userTopic.pipeInput("u1", new UserRegistered("u1", "1"));
        userTopic.pipeInput("u2", new UserRegistered("u2", "1"));
        userTopic.pipeInput("u3", new UserRegistered("u3", "2"));
        userTopic.pipeInput("u4", new UserRegistered("u4", "3"));

        addressTopic.pipeInput("1", new AddressRegistered("1", "Street1", 111, "City1", "Country1"));
        addressTopic.pipeInput("2", new AddressRegistered("2", "Street2", 222, "City2", "Country2"));
        addressTopic.pipeInput("3", new AddressRegistered("3", "Street3", 333, "City3", "Country3"));

        var joinedTopicList = testDriver.createOutputTopic(JOINED_TOPIC, new StringDeserializer(), UserAddressJoin.joinedSerde(MOCK_SR).deserializer()).readRecordsToList();

        assertEquals(4, joinedTopicList.size());

    }

    @Test
    public void usersAndAddressesCannotBeJoined_AddressNotExisting() {

        var userTopic = testDriver.createInputTopic(USER_TOPIC, new StringSerializer(), UserAddressJoin.userSerde(MOCK_SR).serializer());
        var addressTopic = testDriver.createInputTopic(ADDRESS_TOPIC, new StringSerializer(), UserAddressJoin.addressSerde(MOCK_SR).serializer());

        userTopic.pipeInput("u1", new UserRegistered("u1", "1"));
        userTopic.pipeInput("u2", new UserRegistered("u2", "1"));
        userTopic.pipeInput("u3", new UserRegistered("u3", "222"));
        userTopic.pipeInput("u4", new UserRegistered("u4", "3"));

        addressTopic.pipeInput("1", new AddressRegistered("1", "Street1", 111, "City1", "Country1"));
        addressTopic.pipeInput("2", new AddressRegistered("2", "Street2", 222, "City2", "Country2"));
        addressTopic.pipeInput("3", new AddressRegistered("3", "Street3", 333, "City3", "Country3"));

        var joinedTopicList = testDriver.createOutputTopic(JOINED_TOPIC, new StringDeserializer(), UserAddressJoin.joinedSerde(MOCK_SR).deserializer()).readRecordsToList();

        assertEquals(3, joinedTopicList.size());

    }

}
