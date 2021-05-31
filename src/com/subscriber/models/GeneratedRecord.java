package com.subscriber.models;

import java.util.HashMap;

public class GeneratedRecord {

    private HashMap<String, Object> tupleData;
    private String relationName;
    private String type;

    public void setTupleData(HashMap<String, Object> tupleData) {
        this.tupleData = tupleData;
    }

    public void setRelationName(String relationName) {
        this.relationName = relationName;
    }

    public void setType(String type) {
        this.type = type;
    }

    public HashMap<String, Object> getTupleData() {
        return tupleData;
    }

    public String getRelationName() {
        return relationName;
    }

    public String getType() {
        return type;
    }
}