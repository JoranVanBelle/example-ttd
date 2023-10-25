package org.example.infra;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.AddressRegistered;

import java.util.Properties;
import java.util.Scanner;

public class AddressProducer {

    private static Scanner input;

    public static void main(String [] args) {

        input = new Scanner(System.in);

        try(Producer<String, AddressRegistered> addressProducer = new KafkaProducer<>(getProps())) {

            while(true) {
                var addressRegistered = getAddress();

                ProducerRecord<String, AddressRegistered> record = new ProducerRecord<>("example.address", addressRegistered.getAddressId(), addressRegistered);
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
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");

        return props;
    }

    public static AddressRegistered getAddress() {
        AddressRegistered address = new AddressRegistered();

        System.out.println("\nEnter address data");
        System.out.print("addressId: ");
        address.setAddressId(input.next());
        System.out.print("Street: ");
        address.setStreet(input.next());
        System.out.print("number: ");
        address.setNumber(Integer.parseInt(input.next()));
        System.out.print("city: ");
        address.setCity(input.next());
        System.out.print("country: ");
        address.setCountry(input.next());

        return address;
    }

}
