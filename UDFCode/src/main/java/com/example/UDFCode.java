package com.example;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class UDFCode extends UDF {

    private static final Logger logger = Logger.getLogger(UDFCode.class);
    private static Map<String, String> codeMap = new HashMap<>();

    static {
        try {
            ObjectMapper mapper = new ObjectMapper();
            InputStream is = UDFCode.class.getClassLoader().getResourceAsStream("Code.json");
            if (is != null) {
                codeMap = mapper.readValue(is, new TypeReference<Map<String, String>>() {});
            } else {
                logger.error("Failed to find Code.json in classpath");
            }
        } catch (IOException e) {
            logger.error("Failed to load code from Code.json", e);
        }
    }

    public String evaluate(String code) {
        if (code == null || code.trim().isEmpty()) {
            logger.warn("Received null or empty code");
            return null;
        }

        String codeStr = code.trim();
        String codeName = codeMap.getOrDefault(codeStr, codeStr);
        
        return codeName;
    }
}
