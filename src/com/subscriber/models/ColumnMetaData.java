package com.subscriber.models;

public class ColumnMetaData {
    private final String columnName;
    private final int columnSize;
    private final int decimalDigits;
    private final String typeName;
    private final int dataType;
    private final boolean isNullable;
    private final boolean isAutoIncrement;

    public ColumnMetaData(String columnName, int columnSize, int decimalDigits, String typeName, int dataType, boolean isNullable, boolean isAutoIncrement) {
        this.columnName = columnName;
        this.columnSize = columnSize;
        this.decimalDigits = decimalDigits;
        this.typeName = typeName;
        this.dataType = dataType;
        this.isNullable = isNullable;
        this.isAutoIncrement = isAutoIncrement;
    }

    public String getColumnName() {
        return columnName;
    }

    public int getColumnSize() {
        return columnSize;
    }

    public String getTypeName() {
        return typeName;
    }

    public int getDataType() {
        return dataType;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public boolean isAutoIncrement() {
        return isAutoIncrement;
    }

    public int getDecimalDigits() {
        return decimalDigits;
    }
}
