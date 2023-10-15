package com.ztgx.nifi.util;

import com.alibaba.fastjson.JSONObject;
import com.ztgx.nifi.processor.entity.CTResponse;
import org.springframework.http.MediaType;

public class RuleUtil {

    public static  CTResponse postRule(String params, String cleanURI)
    {
        String restUrl = cleanURI;
        JSONObject jsonObj = JSONObject.parseObject(params);
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
        headers.setContentType(type);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());

        org.springframework.http.HttpEntity<String> formEntity = new org.springframework.http.HttpEntity<String>(
                jsonObj.toString(), headers);
        CTResponse resp = new CTResponse();
        try{
            resp = RestClient.getClient().postForObject(restUrl, formEntity, CTResponse.class);

        }catch(Exception e){
            e.printStackTrace();
        }

        return resp;
    }
//
//    public static void main(String[] args) {
//        String params = "{\"parameter1\":\"913501006830690573\"}";
//        String url = "http://192.168.2.86:9013/rs/rs/v0031";
//        CTResponse r = postRule(params,url);
//        System.out.println(r.getResult());
//    }

}
