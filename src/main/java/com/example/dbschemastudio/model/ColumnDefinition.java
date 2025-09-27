package com.example.dbschemastudio.model;

public record ColumnDefinition(String name, String typeClause) {
    public ColumnDefinition {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Column name must not be blank");
        }
        if (typeClause == null || typeClause.isBlank()) {
            throw new IllegalArgumentException("Column definition must include a type");
        }
    }
}
