package com.example;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

public class UDFArea extends UDF {

    private static final Logger logger = Logger.getLogger(UDFArea.class);
    private static Map<String, String> regionMap = Collections.emptyMap();

    static {
        try {
            ObjectMapper mapper = new ObjectMapper();
            InputStream is = UDFArea.class.getClassLoader().getResourceAsStream("Area.json");
            if (is != null) {
                regionMap = mapper.readValue(is, new TypeReference<Map<String, String>>() {});
            } else {
                logger.error("Failed to find Area.json in classpath");
            }
        } catch (IOException e) {
            logger.error("Failed to load region codes from Area.json", e);
        }
    }

    public String evaluate(String idCard, int length) {
        if (idCard == null) {
            logger.warn("Received null ID card");
            return null;
        }

        if (idCard.length() < length) {
            logger.warn("Invalid ID card: " + idCard);
            return "Invalid ID Card";
        }

        String regionCode = idCard.substring(0, length);
        String regionName = regionMap.getOrDefault(regionCode, "");

        return regionName;
    }
}
