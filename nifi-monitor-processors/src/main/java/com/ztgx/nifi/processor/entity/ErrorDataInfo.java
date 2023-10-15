package com.ztgx.nifi.processor.entity;

import java.util.Date;

public class ErrorDataInfo {


    private String id ;
    private String columnName;
    private  String columnValue;
    private String description;
    private Date executeTIme;
    private String errorType;
    private String errorGrade;


    public String getErrorType() {
        return errorType;
    }

    public void setErrorType(String errorType) {
        this.errorType = errorType;
    }



    public String getErrorGrade() {
        return errorGrade;
    }

    public void setErrorGrade(String errorGrade) {
        this.errorGrade = errorGrade;
    }




    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
    public String getColumnValue() {
        return columnValue;
    }

    public void setColumnValue(String columnValue) {
        this.columnValue = columnValue;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Date getExecuteTIme() {
        return executeTIme;
    }

    public void setExecuteTIme(Date executeTIme) {
        this.executeTIme = executeTIme;
    }
}
