package com.subscriber.models;

import com.subscriber.models.ColumnMetaData;

import java.util.List;

public class TableMetaData {
    private final String name;
    private final List<ColumnMetaData> columnMetaDataList;

    public TableMetaData(String name, List<ColumnMetaData> columnMetaDataList) {
        this.name = name;
        this.columnMetaDataList = columnMetaDataList;
    }

    public String getName() {
        return name;
    }

    public List<ColumnMetaData> getColumnMetaDataList() {
        return columnMetaDataList;
    }
}
