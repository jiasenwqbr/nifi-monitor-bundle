package com.ztgx.nifi.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ztgx.nifi.processor.entity.CTResponse;
import com.ztgx.nifi.processor.entity.DirtyData;
import com.ztgx.nifi.processor.entity.ErrorDataInfo;

import java.util.*;

/**
 * @Description:
 * @author: 贾森
 * @date: 2021年12月30日 上午11:49
 */
public class QualidateValidate {
    public static  DirtyData validateRow(String validateRuleStr, String validateMuliRuleStr, JSONObject jsonObject) {
        DirtyData dd = new DirtyData();
        //dd.setDatapri(jsonObject.getString("ID"));
        if (jsonObject.getString("ID")!=null){
            dd.setDatapri(jsonObject.getString("ID"));
        }else if (jsonObject.getString("id")!=null){
            dd.setDatapri(jsonObject.getString("id"));
        }
        List<ErrorDataInfo> errorDataList = new ArrayList<>();
        System.out.println("validateRuleStr:"+validateRuleStr);
        JSONArray ja = JSONArray.parseArray(validateRuleStr);
        //单字段规则校验
        for (int i = 0; i < ja.size(); i++) {
            String fieldname = ja.getJSONObject(i).getString("fieldname");
            String fieldnameCN = ja.getJSONObject(i).getString("fieldnameCN");
            String rule = ja.getJSONObject(i).getString("rule");
            String trueStr = ja.getJSONObject(i).getString("trueStr");
            String falseStr = ja.getJSONObject(i).getString("falseStr");
            String emptyStr = ja.getJSONObject(i).getString("emptyStr");
            String parameter = ja.getJSONObject(i).getString("parameter");
            if (jsonObject.containsKey(fieldname)){
                String fieldValue = jsonObject.getString(fieldname);
                if (fieldValue != null && !"".equals(fieldValue)) {
                    //进行验证
                    parameter = parameter.replace("{" + fieldname.toUpperCase() + "}", fieldValue);
                    CTResponse resp = RuleUtil.postRule(parameter, rule);
                    if (resp.getResult() != null) {
                        if (resp.getResult().toString().equals("false")) {
                            ErrorDataInfo edi = new ErrorDataInfo();
                            edi.setColumnName(fieldname);
                            edi.setColumnValue(fieldValue);
                            if(falseStr==null){
                                JSONObject jsonObject1 = (JSONObject) JSONObject.parse(parameter);
                                String params = jsonObject1.getString("params");
                                if(params!=null){
                                    JSONObject jsonObject2 = (JSONObject) JSONObject.parse(params);
                                    falseStr = fieldname +":" + fieldValue +","+jsonObject2.getString("errorMsg");
                                }
                            }
                            edi.setDescription(falseStr);
                            edi.setExecuteTIme(new Date());
                            edi.setId(UUID.randomUUID().toString());
                            errorDataList.add(edi);
                        }
                    }
                }
            }
        }

        //多字段规则校验
        if(validateMuliRuleStr!=null&&!"".equals(validateMuliRuleStr)&&!"null".equals(validateMuliRuleStr)){
            Set<String> fieldSet = new HashSet<String>();
            for (String s : jsonObject.keySet()){
                fieldSet.add(s.toUpperCase(Locale.ROOT));
            }
            JSONArray jsonArray = JSONArray.parseArray(validateMuliRuleStr);
            for (int i = 0; i<jsonArray.size();i++){
                JSONObject jsonObject1 = jsonArray.getJSONObject(i);
                String fieldnames = jsonObject1.getString("fieldnames");
                Set<String> set1 = new HashSet<>();
                for (String s : fieldnames.split(",")){
                    set1.add(s);
                }
                String rule = jsonObject1.getString("rule");
                String fieldnameCNs = jsonObject1.getString("fieldnameCNs");
                String flaseStr = jsonObject1.getString("flaseStr");
                String parameter = jsonObject1.getString("parameter");
                String values = "";

                if (fieldSet.containsAll(set1)){
                    for (String s : set1){

                        String value = "";

                        if (jsonObject.get(s)!=null){
                            value = jsonObject.getString(s);
                        }else{
                            if (jsonObject.get(s.toLowerCase(Locale.ROOT))!=null){
                                value = jsonObject.getString(s.toLowerCase(Locale.ROOT));
                            }
                        }
                        parameter = parameter.replace("{"+s+"}",value);
                        values += value+",";
                    }

                    CTResponse resp = RuleUtil.postRule(parameter, rule);
                    if (resp.getResult() != null) {
                        if (resp.getResult().toString().equals("false")) {
                            ErrorDataInfo edi = new ErrorDataInfo();
                            edi.setColumnName(fieldnames);
                            edi.setColumnValue(values.substring(0,values.length()-1));
                            edi.setDescription(flaseStr);
                            edi.setExecuteTIme(new Date());
                            edi.setId(UUID.randomUUID().toString());
                            errorDataList.add(edi);
                        }
                    }
                }
            }

        }
        dd.setErrorDataList(errorDataList);
        dd.setRowData(jsonObject.toJSONString());

        return dd;
    }

}
