package bbejeck.streams.itconnect;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Date;

public class Tweet {
    @JsonProperty
    private String text;
    @JsonProperty
    private long retweet_count;
    @JsonProperty
    private long reply_count;
    @JsonProperty
    private long timestamp_ms;

    public String getText() {
        return text;
    }

    public long getRetweetCount() {
        return retweet_count;
    }

    public long getReplyCount() {
        return reply_count;
    }

    public Instant timestamp() {
        return Instant.ofEpochMilli(timestamp_ms);
    }

    @Override
    public String toString() {
        return "Tweet{" +
                "text='" + text + '\'' +
                ", retweetCount='" + retweet_count + '\'' +
                ", replyCount='" + reply_count + '\'' +
                ", timestamp='" + timestamp() + '\'' +
                '}';
    }
}
