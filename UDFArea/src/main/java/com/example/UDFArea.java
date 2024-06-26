package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import java.io.File;
import java.io.IOException;
import java.util.Map;

public class UDFArea extends UDF {

    private static final Logger logger = Logger.getLogger(UDFArea.class);
    private static Map<String, String> regionMap;
    
    static {
        try {
            ObjectMapper mapper = new ObjectMapper();
            regionMap = mapper.readValue(new File("Area.json"), Map.class);
        } catch (IOException e) {
            logger.error("Failed to load region codes from Area.json", e);
            throw new RuntimeException("Failed to load region codes", e);
        }
    }
    
    public Text evaluate(Text idCard) {
        if (idCard == null) {
            logger.warn("Received null ID card");
            return null;
        }
        
        String idCardStr = idCard.toString();
        if (idCardStr.length() < 6) {
            logger.warn("Invalid ID card: " + idCardStr);
            return new Text("Invalid ID Card");
        }

        String regionCode = idCardStr.substring(0, 6);
        String regionName = regionMap.getOrDefault(regionCode, "Unknown Region");
        
        return new Text(regionName);
    }
}
