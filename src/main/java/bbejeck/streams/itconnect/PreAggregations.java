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
        KStream<Long, String> tweetStreamWithRetweetKey = tweetTopicStream.map(mapperToRetweetKey());
        KGroupedStream<Long, String> tweetStreamGroupByRetweetKey = tweetStreamWithRetweetKey.groupByKey(Serdes.Long(), Serdes.String());
        KTable<Long, Long> countByRetweets = tweetStreamGroupByRetweetKey.count();
        countByRetweets.toStream().print();
        countByRetweets.to(Serdes.Long(), Serdes.Long(), "counts");
        return streamBuilder;
    }

    private static void run(KStreamBuilder streamBuilder) {
        Properties props = new Properties();
        props.put("application.id", UUID.randomUUID().toString());
        props.put("bootstrap.servers", "localhost:9092");
        new KafkaStreams(streamBuilder, props).start();
    }

    private static KeyValueMapper<String, String, KeyValue<Long, String>> mapperToRetweetKey() {
        return (String key, String json) -> {
            Map<String, Object> tweet = toMap(json);
            System.out.println(tweet.get("reply_count")+" -- "+json);
            return new KeyValue<Long, String>(retweetCount(tweet), json);
        };
    }

    private static Long retweetCount(Map<String, Object> tweet) {
        return extract(tweet, "retweet_count")
                + extract(tweet, "reply_count");
    }

    private static Long extract(Map<String, Object> tweet, String retweetCountKey) {
        return new Long(tweet.getOrDefault(retweetCountKey, "0").toString());
    }

    private static Map<String, Object> toMap(String json) {
        try {
            return mapReader.readValue(json);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
