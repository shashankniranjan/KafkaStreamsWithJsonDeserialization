package com.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class StreamsStarterApp {

    public static void main(String[] args) {


        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, JsonDeserializer.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonDeserializer.JsonNode().getClass().getName());


        StreamsBuilder builder = new StreamsBuilder();


        KStream<String, JsonNode> read = builder.stream("Test");

        KStream<String, JsonNode> filtered = read.filter((k, v) -> (v.get("door").asText().equals("open")));
        filtered.print(Printed.toSysOut());

        filtered.to("Filtered", Produced.with(Serdes.String(), JsonDeserializer.JsonNode()));

		KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

    }

}
