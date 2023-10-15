package com.ztgx.nifi.processor.entity;

import com.alibaba.fastjson.JSONObject;

import java.util.List;

public class DirtyData {
    private String id;
    private String rowData;
    private String sessionId;
    private String datapri;
    private List<ErrorDataInfo> errorDataList;
    private JSONObject jsonObject;

    public JSONObject getJsonObject() {
        return jsonObject;
    }

    public void setJsonObject(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
    }
    public String getRowData() {
        return rowData;
    }

    public void setRowData(String rowData) {
        this.rowData = rowData;
    }



    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }



    public List<ErrorDataInfo> getErrorDataList() {
        return errorDataList;
    }

    public void setErrorDataList(List<ErrorDataInfo> errorDataList) {
        this.errorDataList = errorDataList;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getDatapri() {
        return datapri;
    }

    public void setDatapri(String datapri) {
        this.datapri = datapri;
    }

}
