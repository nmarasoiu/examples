package io.confluent.examples.connectandstreams.itconnect;

import com.satori.rtm.*;
import com.satori.rtm.model.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SatoriImporter {
    public static void main(String[] args) {
        SubscriptionAdapter listener = listener();
        RtmClient client = rtmClient();

        //subscribe
        String channelName = "Twitter-statuses-sample";
        client.createSubscription(channelName, SubscriptionMode.SIMPLE, listener);
        // Connects the client to RTM
        client.start();
    }

    private static RtmClient rtmClient() {
        return new RtmClientBuilder("wss://open-data.api.satori.com", "b4B4c0f115BAfEe6AF8cB5b72D7bE4EC")
                // Sets a listener for RTM lifecycle events
                .setListener(new RtmClientAdapter() {
                    // When the client successfully connects to RTM
                    public void onEnterConnected(RtmClient client1) {
                        System.out.println("Connected to Satori RTM!");
                    }
                })
                // Builds the client instance
                .build();
    }

    private static SubscriptionAdapter listener() {
        Producer<String, String> producer = kafkaProducer();
        return new SubscriptionAdapter() {
            @Override
            public void onEnterSubscribed(SubscribeRequest request, SubscribeReply reply) {
                System.out.println("Subscribed to: " + reply.getSubscriptionId());
            }

            @Override
            public void onLeaveSubscribed(SubscribeRequest request, SubscribeReply reply) {
                System.out.println("Unsubscribed from: " + reply.getSubscriptionId());
            }

            @Override
            public void onSubscriptionError(SubscriptionError error) {
                String txt = String.format(
                        "Subscription failed. RTM sent the error %s: %s", error.getError(), error.getReason());
                System.out.println(txt);
            }

            @Override
            public void onSubscriptionData(SubscriptionData data) {
                for (AnyJson json : data.getMessages()) {
                    producer.send(new ProducerRecord<String, String>("tweets", json.toString()));
                }
            }
        };
    }
    private static Producer<String, String> kafkaProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(props);
    }
}
