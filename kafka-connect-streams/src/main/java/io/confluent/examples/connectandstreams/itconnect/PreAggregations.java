package io.confluent.examples.connectandstreams.itconnect;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.confluent.examples.connectandstreams.utils.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Properties;

public class PreAggregations {
    private static final ObjectReader tweetReader = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .readerFor(Tweet.class);
    private static File USER_SCHEMA = new File("/Users/nmarasoiu/open/kafka-eco/kafka-confluent-examples/examples/kafka-connect-streams/src/main/java/io/confluent/examples/connectandstreams/itconnect/lang_stats.avro_schema.json");

    public static void main(String[] args) throws IOException {
        run(builder());
    }

    private static KStreamBuilder builder() throws IOException {
        Schema schema = new Schema.Parser().parse(USER_SCHEMA);

        KStreamBuilder streamBuilder = new KStreamBuilder();
        KStream<String, String> tweetTopicStream = streamBuilder.stream(Serdes.String(), Serdes.String(), "tweets");
        KStream<String, String> tweetStreamWithLanguage = tweetTopicStream.map(mapperToLanguage());
        KGroupedStream<String, String> tweetStreamGroupByLanguage = tweetStreamWithLanguage.groupByKey(Serdes.String(), Serdes.String());
        KTable<String, Long> countByRetweets = tweetStreamGroupByLanguage.count();
        countByRetweets.toStream().print();
        KTable<String, GenericRecord> countByRetweetsAvro = countByRetweets.mapValues(count -> {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("count", count);
            return record;
        });
        countByRetweetsAvro.to(
                Serdes.String(),null,//uses DEFAULT_VALUE_SERDE_CLASS_CONFIG: GenericAvroSerde and "schema.registry.url"
                "language3_stats");
        return streamBuilder;
    }

    private static void run(KStreamBuilder streamBuilder) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-group");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        props.put("schema.registry.url", "http://localhost:8081");
        new KafkaStreams(streamBuilder, props).start();
    }

    private static KeyValueMapper<String, String, KeyValue<String, String>> mapperToLanguage() {
        return (String key, String json) -> {
            Tweet tweet = toTweet(json);
            return new KeyValue<String, String>(tweet.getLanguage(), json);
        };
    }

    private static Tweet toTweet(String json) {
        try {
            return tweetReader.readValue(json);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
