package com.models;

public class PubSubTableInfo {
    private final String pubSchemaName;
    private final String pubTableName;
    private final String pubUniqueIndex;
    private final String pubCallInfoFieldName;

    private final String subSchemaName;
    private final String subTableName;

    public String getPubSchemaName() {
        return pubSchemaName;
    }

    public String getPubTableName() {
        return pubTableName;
    }

    public String getPubUniqueIndex() {
        return pubUniqueIndex;
    }

    public String getPubCallInfoFieldName() {
        return pubCallInfoFieldName;
    }

    public String getSubSchemaName() {
        return subSchemaName;
    }

    public String getSubTableName() {
        return subTableName;
    }

    public String getPubFullTableName() {
        return pubSchemaName + "." + pubTableName;
    }

    public String getSubFullTableName() {
        return subSchemaName + "." + subTableName;
    }

    public PubSubTableInfo(String pubSchemaName, String pubTableName, String pubUniqueIndex, String pubCallInfoFieldName, String subSchemaName, String subTableName) {
        this.pubSchemaName = pubSchemaName;
        this.pubTableName = pubTableName;
        this.pubUniqueIndex = pubUniqueIndex;
        this.pubCallInfoFieldName = pubCallInfoFieldName;
        this.subSchemaName = subSchemaName;
        this.subTableName = subTableName;
    }
}
