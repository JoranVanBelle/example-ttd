package org.example.infra;

import AddressesExample.org.example.UserRegistered;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.AddressRegistered;

import java.util.Properties;
import java.util.Scanner;

public class UserProducer {

    private static Scanner input;

    public static void main(String [] args) {

        input = new Scanner(System.in);

        try(Producer<String, UserRegistered> addressProducer = new KafkaProducer<>(getProps())) {

            while(true) {
                var userRegistered = getUser();

                ProducerRecord<String, UserRegistered> record = new ProducerRecord<>("example.user", userRegistered.getUserid(), userRegistered);
                addressProducer.send(record);
                System.out.println("Send");

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static Properties getProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");

        return props;
    }

    public static UserRegistered getUser() {
        UserRegistered user = new UserRegistered();

        System.out.println("\nEnter address data");
        System.out.print("userId: ");
        user.setUserid(input.next());
        System.out.print("addressId: ");
        user.setAddressId(input.next());

        return user;
    }
}
