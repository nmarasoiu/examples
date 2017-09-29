package io.confluent.examples.connectandstreams.itconnect;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Tweet {
    @JsonProperty
    private String text;
    @JsonProperty("retweet_count")
    long retweetCount;
    @JsonProperty("reply_count")
    long replyCount;
    @JsonProperty("timestamp_ms")
    private long timestamp;
    @JsonProperty("lang")
    private String language;

    public String getText() {
        return text;
    }

    public long getRetweetCount() {
        return retweetCount;
    }

    public long getReplyCount() {
        return replyCount;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getLanguage() {
        return language;
    }

    @Override
    public String toString() {
        return "Tweet{" +
                "text='" + text + '\'' +
                ", retweetCount=" + retweetCount +
                ", replyCount=" + replyCount +
                ", timestamp=" + timestamp +
                ", language='" + language + '\'' +
                '}';
    }
}
