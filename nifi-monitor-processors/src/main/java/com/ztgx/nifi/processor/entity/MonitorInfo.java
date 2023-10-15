package com.ztgx.nifi.processor.entity;

import java.util.Date;
import java.util.List;

public class MonitorInfo {

    //id
    private String id;
    //错误记录条数
    private int errorCount;
    //正确记录条数
    private int rightCount;
    //执行开始时间
    private Date excuteTime;
    //执行结束时间
    private Date excuteEndTime;
    //异常报错信息
    private String exception;

    private String  policyId;
    private String policyType;
    private String databaseName;
    private String sourceTableName;
    private String policyName;
    private String destinationTableName;
    private String processorType;
    //关联字段
    private String sessionId;
    private String runNifiId;



    private String nifiGroupId;
    //错误数据信息
    private List<DirtyData> dirtyDataList;
    public String getNifiGroupId() {
        return nifiGroupId;
    }

    public void setNifiGroupId(String nifiGroupId) {
        this.nifiGroupId = nifiGroupId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String  getPolicyId() {
        return policyId;
    }

    public void setPolicyId(String  policyId) {
        this.policyId = policyId;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getPolicyName() {
        return policyName;
    }

    public void setPolicyName(String policyName) {
        this.policyName = policyName;
    }

    public String getDestinationTableName() {
        return destinationTableName;
    }

    public void setDestinationTableName(String destinationTableName) {
        this.destinationTableName = destinationTableName;
    }

    public String getProcessorType() {
        return processorType;
    }

    public void setProcessorType(String processorType) {
        this.processorType = processorType;
    }

    public int getErrorCount() {
        return errorCount;
    }

    public void setErrorCount(int errorCount) {
        this.errorCount = errorCount;
    }

    public int getRightCount() {
        return rightCount;
    }

    public void setRightCount(int rightCount) {
        this.rightCount = rightCount;
    }


    public Date getExcuteTime() {
        return excuteTime;
    }

    public void setExcuteTime(Date excuteTime) {
        this.excuteTime = excuteTime;
    }

    public String getException() {
        return exception;
    }

    public void setException(String exception) {
        this.exception = exception;
    }

    public List<DirtyData> getDirtyDataList() {
        return dirtyDataList;
    }

    public void setDirtyDataList(List<DirtyData> dirtyDataList) {
        this.dirtyDataList = dirtyDataList;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getPolicyType() {
        return policyType;
    }

    public void setPolicyType(String policyType) {
        this.policyType = policyType;
    }

    public String getRunNifiId() {
        return runNifiId;
    }

    public Date getExcuteEndTime() {
        return excuteEndTime;
    }

    public void setExcuteEndTime(Date excuteEndTime) {
        this.excuteEndTime = excuteEndTime;
    }

    public void setRunNifiId(String runNifiId) {
        this.runNifiId = runNifiId;
    }


}
