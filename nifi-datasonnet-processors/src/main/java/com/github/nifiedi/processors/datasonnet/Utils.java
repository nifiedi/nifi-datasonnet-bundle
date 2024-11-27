package com.github.nifiedi.processors.datasonnet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.datasonnet.document.MediaType;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * @author Chaojun.Xu
 * @date 2024/11/27 17:15
 */


public class Utils {
    private static ObjectMapper objectMapper = new ObjectMapper();

    public static File asFile(String f) {
        if (f == null || f.length() == 0) {
            return null;
        }
        return new File(f);
    }

    public static MediaType getMediaType(String mediaType) {
        if (mediaType == null || mediaType.length() == 0) {
            throw new IllegalArgumentException("mediaType is null or empty");
        }
        return MediaType.valueOf(mediaType);
    }

    public static Object getMapFromJson(String json) throws  JsonProcessingException {
        if (json == null || json.length() == 0) {
            return null;
        }
        JsonNode jsonNode = objectMapper.readTree(json);
        if(jsonNode.isArray()){

            return objectMapper.convertValue(jsonNode, List.class);
        }
        return objectMapper.convertValue(jsonNode, Map.class);
    }
}