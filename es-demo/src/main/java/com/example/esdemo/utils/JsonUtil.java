package com.example.esdemo.utils;

import com.example.esdemo.bean.CustomerEsBean;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * json工具类
 *
 * @author WangBoran
 * @since 2020/9/1 17:46
 */
public class JsonUtil {

    public static String CustomerEsBeanToJson(CustomerEsBean customer) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        String jsonString = mapper.writeValueAsString(customer);
        return jsonString.replace("accountNumber", "account_number")
                .replace("firstName", "firstname")
                .replace("lastName", "lastname");
    }

    public static Object jsonToObj(Object obj, String jsonStr) throws JsonParseException, JsonMappingException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(jsonStr, obj.getClass());
    }

    public static String objToJson(Object obj) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(obj);
    }
}
