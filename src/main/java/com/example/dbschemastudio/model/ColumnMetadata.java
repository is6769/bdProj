package com.example.dbschemastudio.model;

public record ColumnMetadata(String name, String typeName, boolean nullable, boolean autoIncrement) {
}
