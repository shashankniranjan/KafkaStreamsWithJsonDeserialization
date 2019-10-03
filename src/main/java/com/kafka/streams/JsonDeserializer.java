package com.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;

public class JsonDeserializer extends Serdes {
    public static Serde<JsonNode> JsonNode() {
        return new JsonDeserializer.JsonNodeSerde();
    }

    public static final class JsonNodeSerde extends Serdes.WrapperSerde<JsonNode> {
        public JsonNodeSerde() {
            super(new JsonNodeSerializer(), new JsonNodeCustomDeserializer());
        }

        private static class JsonNodeSerializer implements Serializer<JsonNode> {


            @Override
            public byte[] serialize(String s, JsonNode jsonNode) {
                return jsonNode.asText().getBytes();
            }
        }

        private static class JsonNodeCustomDeserializer implements Deserializer<JsonNode> {
            @Override
            public JsonNode deserialize(String s, byte[] bytes) {
                ObjectMapper objectMapper = new ObjectMapper();
                try {
                    return objectMapper.readTree(bytes);
                } catch (IOException e) {

                    e.printStackTrace();

                    // return something here
                    return null;
                }
            }
        }
    }

}
