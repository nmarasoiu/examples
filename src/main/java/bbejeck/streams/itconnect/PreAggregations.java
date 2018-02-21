package bbejeck.streams.itconnect;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class PreAggregations {
    private static final ObjectReader mapReader = new ObjectMapper().readerFor(Map.class);

    public static void main(String[] args) {
        run(builder());
    }

    private static KStreamBuilder builder() {
        KStreamBuilder streamBuilder = new KStreamBuilder();
        KStream<String, String> tweetTopicStream = streamBuilder.stream(Serdes.String(), Serdes.String(), "tweets");
        KStream<String, String> tweetStreamWithRetweetKey = tweetTopicStream.map(mapperToRetweetKey());
        KGroupedStream<String, String> tweetStreamGroupByRetweetKey = tweetStreamWithRetweetKey.groupByKey(Serdes.String(), Serdes.String());
        KTable<String, Long> countByLanguage = tweetStreamGroupByRetweetKey.count();
        countByLanguage.toStream().print();
        countByLanguage.to(Serdes.String(), Serdes.Long(), "counts");
        return streamBuilder;
    }

    private static void run(KStreamBuilder streamBuilder) {
        Properties props = new Properties();
        props.put("application.id", UUID.randomUUID().toString());
        props.put("bootstrap.servers", "localhost:9092");
        new KafkaStreams(streamBuilder, props).start();
    }

    private static KeyValueMapper<String, String, KeyValue<String, String>> mapperToRetweetKey() {
        return (String key, String json) -> {
            Map<String, Object> tweet = toMap(json);
            return new KeyValue<>(String.valueOf(tweet.get("lang")), json);
        };
    }

    private static Map<String, Object> toMap(String json) {
        try {
            return mapReader.readValue(json);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
