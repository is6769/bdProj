package com.example.dbschemastudio.model;

import java.util.Optional;

public record DataFilter(String column, String operator, String value) {
    private static final String[] SUPPORTED_OPERATORS = {"=", "!=", ">", "<", ">=", "<=", "LIKE", "ILIKE"};

    public DataFilter {
        if (column == null || column.isBlank()) {
            throw new IllegalArgumentException("Filter column must not be blank");
        }
        operator = Optional.ofNullable(operator).map(String::trim).orElse("=");
        boolean allowed = false;
        for (String supportedOperator : SUPPORTED_OPERATORS) {
            if (supportedOperator.equalsIgnoreCase(operator)) {
                operator = supportedOperator;
                allowed = true;
                break;
            }
        }
        if (!allowed) {
            throw new IllegalArgumentException("Unsupported operator: " + operator);
        }
    }
}
