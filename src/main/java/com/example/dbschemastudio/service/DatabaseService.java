package com.example.dbschemastudio.service;

import com.example.dbschemastudio.model.ColumnDefinition;
import com.example.dbschemastudio.model.ColumnMetadata;
import com.example.dbschemastudio.model.DataFilter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class DatabaseService {

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate namedTemplate;
    private final PlatformTransactionManager transactionManager;
    private final SqlLogService logService;
    private final AtomicReference<TransactionStatus> currentTransaction = new AtomicReference<>();

    public DatabaseService(DataSource dataSource,
                           JdbcTemplate jdbcTemplate,
                           NamedParameterJdbcTemplate namedTemplate,
                           PlatformTransactionManager transactionManager,
                           SqlLogService logService) {
        this.dataSource = dataSource;
        this.jdbcTemplate = jdbcTemplate;
        this.namedTemplate = namedTemplate;
        this.transactionManager = transactionManager;
        this.logService = logService;
    }

    public boolean testConnection() {
        try (Connection connection = dataSource.getConnection()) {
            return connection.isValid(2);
        } catch (SQLException ex) {
            logService.logError("<connection-test>", ex.getMessage());
            return false;
        }
    }

    public List<String> listTables() {
        Connection conn = DataSourceUtils.getConnection(dataSource);
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            Set<String> tableNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            String schema = safeSchema(conn);
            try (ResultSet rs = metaData.getTables(conn.getCatalog(), schema, "%", new String[]{"TABLE"})) {
                extractTableNames(tableNames, rs);
            }
            try (ResultSet rs = metaData.getTables(conn.getCatalog(), null, "%", new String[]{"TABLE"})) {
                extractTableNames(tableNames, rs);
            }
            return new ArrayList<>(tableNames);
        } catch (SQLException ex) {
            logService.logError("<metadata:listTables>", ex.getMessage());
            throw new RuntimeException("Unable to list tables", ex);
        } finally {
            DataSourceUtils.releaseConnection(conn, dataSource);
        }
    }

    public List<ColumnMetadata> describeTable(String tableName) {
        validateIdentifier(tableName, "table");
        Connection conn = DataSourceUtils.getConnection(dataSource);
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            List<ColumnMetadata> columns = new ArrayList<>();
            String schema = safeSchema(conn);
            try (ResultSet rs = metaData.getColumns(conn.getCatalog(), schema, tableName, "%")) {
                while (rs.next()) {
                    String name = rs.getString("COLUMN_NAME");
                    String typeName = rs.getString("TYPE_NAME");
                    boolean nullable = rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable;
                    boolean autoIncrement = "YES".equalsIgnoreCase(rs.getString("IS_AUTOINCREMENT"));
                    columns.add(new ColumnMetadata(name, typeName, nullable, autoIncrement));
                }
            }
            if (columns.isEmpty()) {
                try (ResultSet rs = metaData.getColumns(conn.getCatalog(), null, tableName, "%")) {
                    while (rs.next()) {
                        String name = rs.getString("COLUMN_NAME");
                        String typeName = rs.getString("TYPE_NAME");
                        boolean nullable = rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable;
                        boolean autoIncrement = "YES".equalsIgnoreCase(rs.getString("IS_AUTOINCREMENT"));
                        columns.add(new ColumnMetadata(name, typeName, nullable, autoIncrement));
                    }
                }
            }
            columns.sort(Comparator.comparing(ColumnMetadata::name));
            return columns;
        } catch (SQLException ex) {
            logService.logError("<metadata:describe>", ex.getMessage());
            throw new RuntimeException("Unable to describe table " + tableName, ex);
        } finally {
            DataSourceUtils.releaseConnection(conn, dataSource);
        }
    }

    public void createEnum(String enumName, List<String> values) {
        validateIdentifier(enumName, "enum type");
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("At least one enum value is required");
        }
        for (String value : values) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("Enum values must not be blank");
            }
        }
        String valuesList = values.stream()
                .map(v -> "'" + v.replace("'", "''") + "'")
                .reduce((a, b) -> a + ", " + b)
                .orElseThrow();
        String sql = "CREATE TYPE " + quoteIdentifier(enumName) + " AS ENUM (" + valuesList + ")";
        executeUpdate(sql, new Object[]{});
    }

    public List<String> listEnums() {
        String sql = "SELECT t.typname FROM pg_type t " +
                     "JOIN pg_namespace n ON t.typnamespace = n.oid " +
                     "WHERE t.typtype = 'e' AND n.nspname = current_schema() " +
                     "ORDER BY t.typname";
        try {
            return jdbcTemplate.queryForList(sql, String.class);
        } catch (Exception ex) {
            logService.logError("<metadata:listEnums>", ex.getMessage());
            throw new RuntimeException("Unable to list enum types", ex);
        }
    }

    public List<String> getEnumValues(String enumName) {
        validateIdentifier(enumName, "enum type");
        String sql = "SELECT e.enumlabel FROM pg_enum e " +
                     "JOIN pg_type t ON e.enumtypid = t.oid " +
                     "WHERE t.typname = ? " +
                     "ORDER BY e.enumsortorder";
        try {
            return jdbcTemplate.queryForList(sql, String.class, enumName);
        } catch (Exception ex) {
            logService.logError("<metadata:getEnumValues>", ex.getMessage());
            throw new RuntimeException("Unable to get enum values for " + enumName, ex);
        }
    }

    public void createTable(String tableName, List<ColumnDefinition> columns) {
        validateIdentifier(tableName, "table");
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("At least one column definition is required");
        }
        String columnSql = columns.stream()
                .map(cd -> quoteIdentifier(cd.name()) + " " + cd.typeClause())
                .reduce((a, b) -> a + ", " + b)
                .orElseThrow();
        String sql = "CREATE TABLE IF NOT EXISTS " + quoteIdentifier(tableName) + " (" + columnSql + ")";
        executeUpdate(sql, new Object[]{});
    }

    public List<Map<String, Object>> fetchData(String tableName, List<DataFilter> filters) {
        validateIdentifier(tableName, "table");
        StringBuilder sql = new StringBuilder("SELECT * FROM " + quoteIdentifier(tableName));
        MapSqlParameterSource params = new MapSqlParameterSource();
        List<DataFilter> activeFilters = filters == null ? List.of() : filters;
        if (!activeFilters.isEmpty()) {
            List<String> clauses = new ArrayList<>();
            for (int i = 0; i < activeFilters.size(); i++) {
                DataFilter filter = activeFilters.get(i);
                String clause = buildFilterClause(filter, i, params);
                if (clause != null) {
                    clauses.add(clause);
                }
            }
            if (!clauses.isEmpty()) {
                sql.append(" WHERE ").append(String.join(" AND ", clauses));
            }
        }
        String logMessage = sql + (params.getValues().isEmpty() ? "" : " :: " + params.getValues());
        logService.logOperation(logMessage);
        try {
            return namedTemplate.queryForList(sql.toString(), params);
        } catch (Exception ex) {
            logService.logError(sql.toString(), ex.getMessage());
            throw ex;
        }
    }

    /**
     * Builds a single filter clause handling special operators like IS NULL, SIMILAR TO, etc.
     */
    private String buildFilterClause(DataFilter filter, int index, MapSqlParameterSource params) {
        String column = quoteIdentifier(filter.column());
        String operator = filter.operator();
        String value = filter.value();
        String paramName = "value" + index;
        
        // Handle IS NULL / IS NOT NULL (no value needed)
        if ("IS NULL".equals(operator) || "IS NOT NULL".equals(operator)) {
            return column + " " + operator;
        }
        
        // Handle SIMILAR TO / NOT SIMILAR TO (pattern as literal)
        if ("SIMILAR TO".equals(operator) || "NOT SIMILAR TO".equals(operator)) {
            // Escape single quotes in pattern
            String escapedPattern = value.replace("'", "''");
            return column + " " + operator + " '" + escapedPattern + "'";
        }
        
        // Handle regex operators
        if (operator != null && (operator.startsWith("~") || operator.startsWith("!~"))) {
            params.addValue(paramName, value);
            return column + " " + operator + " :" + paramName;
        }
        
        // Handle LIKE/ILIKE operators
        if (operator != null && (operator.contains("LIKE"))) {
            params.addValue(paramName, value);
            return column + " " + operator + " :" + paramName;
        }
        
        // Standard comparison operators
        params.addValue(paramName, normalizeValue(value, Optional.of(filter.column())));
        return column + " " + operator + " :" + paramName;
    }

    public List<Map<String, Object>> fetchDataAdvanced(String tableName, 
                                                        List<String> selectColumns,
                                                        List<DataFilter> filters,
                                                        List<String> orderByColumns,
                                                        List<String> orderDirections,
                                                        List<String> groupByColumns,
                                                        List<String> aggregates,
                                                        String havingClause) {
        validateIdentifier(tableName, "table");
        
        // Build SELECT clause
        StringBuilder sql = new StringBuilder("SELECT ");
        if (selectColumns == null) {
            sql.append("*");
        } else {
            List<String> quoted = selectColumns.stream()
                .map(col -> {
                    // Handle aggregate functions or aliases (don't quote those)
                    if (col.contains("(") || col.contains(" AS ")) {
                        return col;
                    }
                    return quoteIdentifier(col);
                })
                .toList();
            sql.append(String.join(", ", quoted));
        }
        
        // Add aggregates if present
        if (aggregates != null && !aggregates.isEmpty()) {
            if (selectColumns != null && !selectColumns.isEmpty()) {
                sql.append(", ");
            }
            sql.append(String.join(", ", aggregates));
        }
        
        sql.append(" FROM ").append(quoteIdentifier(tableName));
        
        // Build WHERE clause
        MapSqlParameterSource params = new MapSqlParameterSource();
        List<DataFilter> activeFilters = filters == null ? List.of() : filters;
        if (!activeFilters.isEmpty()) {
            List<String> clauses = new ArrayList<>();
            for (int i = 0; i < activeFilters.size(); i++) {
                DataFilter filter = activeFilters.get(i);
                String clause = buildFilterClause(filter, i, params);
                if (clause != null) {
                    clauses.add(clause);
                }
            }
            if (!clauses.isEmpty()) {
                sql.append(" WHERE ").append(String.join(" AND ", clauses));
            }
        }
        
        // Build GROUP BY clause
        if (groupByColumns != null && !groupByColumns.isEmpty()) {
            sql.append(" GROUP BY ");
            List<String> quoted = groupByColumns.stream().map(this::quoteIdentifier).toList();
            sql.append(String.join(", ", quoted));
        }
        
        // Build HAVING clause
        if (havingClause != null && !havingClause.isBlank()) {
            sql.append(" HAVING ").append(havingClause);
        }
        
        // Build ORDER BY clause
        if (orderByColumns != null && !orderByColumns.isEmpty()) {
            sql.append(" ORDER BY ");
            List<String> orderClauses = new ArrayList<>();
            for (int i = 0; i < orderByColumns.size(); i++) {
                String column = quoteIdentifier(orderByColumns.get(i));
                String direction = (orderDirections != null && i < orderDirections.size()) 
                    ? orderDirections.get(i) : "ASC";
                orderClauses.add(column + " " + direction);
            }
            sql.append(String.join(", ", orderClauses));
        }
        
        String logMessage = sql + (!params.getValues().isEmpty() ? " :: " + params.getValues() : "");
        logService.logOperation(logMessage);
        
        try {
            return namedTemplate.queryForList(sql.toString(), params);
        } catch (Exception ex) {
            logService.logError(sql.toString(), ex.getMessage());
            throw ex;
        }
    }

    public void insertData(String tableName, Map<String, Object> values, boolean useTransaction) {
        validateIdentifier(tableName, "table");
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("No values to insert");
        }
        Map<String, Object> sanitized = new LinkedHashMap<>();
        values.forEach((key, value) -> {
            if (value != null && !value.toString().isBlank()) {
                sanitized.put(key, normalizeValue(value.toString(), Optional.of(key)));
            }
        });
        if (sanitized.isEmpty()) {
            throw new IllegalArgumentException("All values are blank");
        }
        String columnList = String.join(", ", sanitized.keySet().stream().map(this::quoteIdentifier).toList());
        String paramList = String.join(", ", sanitized.keySet().stream().map(k -> ":" + k).toList());
        String sql = "INSERT INTO " + quoteIdentifier(tableName) + " (" + columnList + ") VALUES (" + paramList + ")";
        MapSqlParameterSource params = new MapSqlParameterSource(sanitized);
        execute(sql, params, useTransaction);
    }

    public void beginTransaction() {
        if (currentTransaction.get() != null) {
            throw new IllegalStateException("A transaction is already active");
        }
        DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
        definition.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        definition.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        TransactionStatus status = transactionManager.getTransaction(definition);
        currentTransaction.set(status);
        logService.logInfo("Transaction started");
    }

    public void commitTransaction() {
        TransactionStatus status = currentTransaction.getAndSet(null);
        if (status == null) {
            throw new IllegalStateException("No active transaction");
        }
        transactionManager.commit(status);
        logService.logInfo("Transaction committed");
    }

    public void rollbackTransaction() {
        TransactionStatus status = currentTransaction.getAndSet(null);
        if (status == null) {
            throw new IllegalStateException("No active transaction");
        }
        transactionManager.rollback(status);
        logService.logInfo("Transaction rolled back");
    }

    public boolean isTransactionActive() {
        return currentTransaction.get() != null;
    }

    private void executeUpdate(String sql, Object[] params) {
        logService.logOperation(sql);
        try {
            jdbcTemplate.update(sql, params);
        } catch (Exception ex) {
            logService.logError(sql, ex.getMessage());
            throw ex;
        }
    }

    private void execute(String sql, MapSqlParameterSource params, boolean useTransaction) {
        logService.logOperation(sql + " :: " + params.getValues());
        try {
            if (useTransaction) {
                TransactionStatus status = currentTransaction.get();
                if (status == null) {
                    throw new IllegalStateException("No active transaction");
                }
                namedTemplate.update(sql, params);
            } else {
                var template = new org.springframework.transaction.support.TransactionTemplate(transactionManager);
                template.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
                template.execute(status -> {
                    namedTemplate.update(sql, params);
                    return null;
                });
            }
        } catch (Exception ex) {
            logService.logError(sql, ex.getMessage());
            throw ex;
        }
    }

    private Object normalizeValue(String value, Optional<String> column) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        if ("true".equalsIgnoreCase(trimmed) || "false".equalsIgnoreCase(trimmed)) {
            return Boolean.parseBoolean(trimmed);
        }
        try {
            return Integer.parseInt(trimmed);
        } catch (NumberFormatException ignored) {
        }
        try {
            return Long.parseLong(trimmed);
        } catch (NumberFormatException ignored) {
        }
        try {
            return Double.parseDouble(trimmed);
        } catch (NumberFormatException ignored) {
        }
        return trimmed;
    }

    private void validateIdentifier(String identifier, String type) {
        if (identifier == null || identifier.isBlank()) {
            throw new IllegalArgumentException(type + " name must not be blank");
        }
        if (!identifier.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
            throw new IllegalArgumentException("Invalid " + type + " name: " + identifier);
        }
    }

    private String quoteIdentifier(String identifier) {
        validateIdentifier(identifier, "identifier");
        return '"' + identifier + '"';
    }

    private void extractTableNames(Set<String> collector, ResultSet rs) throws SQLException {
        while (rs.next()) {
            String tableName = rs.getString("TABLE_NAME");
            if (tableName == null) {
                continue;
            }
            if (tableName.startsWith("pg_") || tableName.startsWith("sql_") || tableName.startsWith("information_schema")) {
                continue;
            }
            collector.add(tableName);
        }
    }

    private String safeSchema(Connection conn) throws SQLException {
        String schema = conn.getSchema();
        if (schema == null || schema.isBlank()) {
            return "public";
        }
        return schema;
    }

    // ========== ALTER TABLE OPERATIONS ==========

    public void renameTable(String oldName, String newName) {
        validateIdentifier(oldName, "table");
        validateIdentifier(newName, "table");
        String sql = "ALTER TABLE " + quoteIdentifier(oldName) + " RENAME TO " + quoteIdentifier(newName);
        try {
            jdbcTemplate.execute(sql);
            logService.logOperation(sql);
        } catch (Exception ex) {
            logService.logError(sql, ex.getMessage());
            throw new RuntimeException("Failed to rename table: " + ex.getMessage(), ex);
        }
    }

    public void addColumn(String tableName, String columnName, String columnType, String constraints) {
        validateIdentifier(tableName, "table");
        validateIdentifier(columnName, "column");
        
        String constraintsPart = (constraints != null && !constraints.isBlank()) ? " " + constraints.trim() : "";
        String sql = "ALTER TABLE " + quoteIdentifier(tableName) + 
                     " ADD COLUMN " + quoteIdentifier(columnName) + " " + columnType + constraintsPart;
        try {
            jdbcTemplate.execute(sql);
            logService.logOperation(sql);
        } catch (Exception ex) {
            logService.logError(sql, ex.getMessage());
            throw new RuntimeException("Failed to add column: " + ex.getMessage(), ex);
        }
    }

    public void dropColumn(String tableName, String columnName) {
        validateIdentifier(tableName, "table");
        validateIdentifier(columnName, "column");
        String sql = "ALTER TABLE " + quoteIdentifier(tableName) + " DROP COLUMN " + quoteIdentifier(columnName);
        try {
            jdbcTemplate.execute(sql);
            logService.logOperation(sql);
        } catch (Exception ex) {
            logService.logError(sql, ex.getMessage());
            throw new RuntimeException("Failed to drop column: " + ex.getMessage(), ex);
        }
    }

    public void renameColumn(String tableName, String oldColumnName, String newColumnName) {
        validateIdentifier(tableName, "table");
        validateIdentifier(oldColumnName, "column");
        validateIdentifier(newColumnName, "column");
        String sql = "ALTER TABLE " + quoteIdentifier(tableName) + 
                     " RENAME COLUMN " + quoteIdentifier(oldColumnName) + " TO " + quoteIdentifier(newColumnName);
        try {
            jdbcTemplate.execute(sql);
            logService.logOperation(sql);
        } catch (Exception ex) {
            logService.logError(sql, ex.getMessage());
            throw new RuntimeException("Failed to rename column: " + ex.getMessage(), ex);
        }
    }

    public void alterColumnType(String tableName, String columnName, String newType, boolean useCast) {
        validateIdentifier(tableName, "table");
        validateIdentifier(columnName, "column");
        
        String usingClause = useCast ? " USING " + quoteIdentifier(columnName) + "::" + newType : "";
        String sql = "ALTER TABLE " + quoteIdentifier(tableName) + 
                     " ALTER COLUMN " + quoteIdentifier(columnName) + " TYPE " + newType + usingClause;
        try {
            jdbcTemplate.execute(sql);
            logService.logOperation(sql);
        } catch (Exception ex) {
            logService.logError(sql, ex.getMessage());
            throw new RuntimeException("Failed to alter column type: " + ex.getMessage(), ex);
        }
    }

    public void addConstraint(String tableName, String constraintName, String constraintDefinition) {
        validateIdentifier(tableName, "table");
        
        String namePart = (constraintName != null && !constraintName.isBlank()) 
                ? "CONSTRAINT " + quoteIdentifier(constraintName) + " " 
                : "";
        String sql = "ALTER TABLE " + quoteIdentifier(tableName) + " ADD " + namePart + constraintDefinition;
        try {
            jdbcTemplate.execute(sql);
            logService.logOperation(sql);
        } catch (Exception ex) {
            logService.logError(sql, ex.getMessage());
            throw new RuntimeException("Failed to add constraint: " + ex.getMessage(), ex);
        }
    }

    public void dropConstraint(String tableName, String constraintName) {
        validateIdentifier(tableName, "table");
        validateIdentifier(constraintName, "constraint");
        String sql = "ALTER TABLE " + quoteIdentifier(tableName) + " DROP CONSTRAINT " + quoteIdentifier(constraintName);
        try {
            jdbcTemplate.execute(sql);
            logService.logOperation(sql);
        } catch (Exception ex) {
            logService.logError(sql, ex.getMessage());
            throw new RuntimeException("Failed to drop constraint: " + ex.getMessage(), ex);
        }
    }

    public void setNotNull(String tableName, String columnName) {
        validateIdentifier(tableName, "table");
        validateIdentifier(columnName, "column");
        String sql = "ALTER TABLE " + quoteIdentifier(tableName) + 
                     " ALTER COLUMN " + quoteIdentifier(columnName) + " SET NOT NULL";
        try {
            jdbcTemplate.execute(sql);
            logService.logOperation(sql);
        } catch (Exception ex) {
            logService.logError(sql, ex.getMessage());
            throw new RuntimeException("Failed to set NOT NULL: " + ex.getMessage(), ex);
        }
    }

    public void dropNotNull(String tableName, String columnName) {
        validateIdentifier(tableName, "table");
        validateIdentifier(columnName, "column");
        String sql = "ALTER TABLE " + quoteIdentifier(tableName) + 
                     " ALTER COLUMN " + quoteIdentifier(columnName) + " DROP NOT NULL";
        try {
            jdbcTemplate.execute(sql);
            logService.logOperation(sql);
        } catch (Exception ex) {
            logService.logError(sql, ex.getMessage());
            throw new RuntimeException("Failed to drop NOT NULL: " + ex.getMessage(), ex);
        }
    }

    // Custom query execution for string functions and other operations
    public List<Map<String, Object>> executeCustomQuery(String sql) {
        logService.logOperation(sql);
        try {
            return jdbcTemplate.queryForList(sql);
        } catch (Exception ex) {
            logService.logError(sql, ex.getMessage());
            throw new RuntimeException("Query failed: " + ex.getMessage(), ex);
        }
    }

    // ========== USER-DEFINED TYPES OPERATIONS ==========

    public void createCompositeType(String typeName, List<String> fieldDefinitions) {
        validateIdentifier(typeName, "type");
        if (fieldDefinitions == null || fieldDefinitions.isEmpty()) {
            throw new IllegalArgumentException("At least one field is required");
        }
        String fieldsSql = String.join(", ", fieldDefinitions);
        String sql = "CREATE TYPE " + quoteIdentifier(typeName) + " AS (" + fieldsSql + ")";
        try {
            jdbcTemplate.execute(sql);
            logService.logOperation(sql);
        } catch (Exception ex) {
            logService.logError(sql, ex.getMessage());
            throw new RuntimeException("Failed to create composite type: " + ex.getMessage(), ex);
        }
    }

    public List<String> listCompositeTypes() {
        String sql = """
            SELECT t.typname 
            FROM pg_type t 
            JOIN pg_namespace n ON t.typnamespace = n.oid 
            WHERE t.typtype = 'c' 
            AND n.nspname = current_schema() 
            AND t.typname NOT LIKE 'pg_%'
            ORDER BY t.typname
            """;
        try {
            return jdbcTemplate.queryForList(sql, String.class);
        } catch (Exception ex) {
            logService.logError(sql, ex.getMessage());
            throw new RuntimeException("Unable to list composite types", ex);
        }
    }

    public List<String> getCompositeTypeFields(String typeName) {
        validateIdentifier(typeName, "type");
        String sql = """
            SELECT a.attname || ' ' || pg_catalog.format_type(a.atttypid, a.atttypmod) as field_def
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            JOIN pg_attribute a ON a.attrelid = t.typrelid
            WHERE t.typname = ?
            AND n.nspname = current_schema()
            AND a.attnum > 0
            ORDER BY a.attnum
            """;
        try {
            return jdbcTemplate.queryForList(sql, String.class, typeName);
        } catch (Exception ex) {
            logService.logError(sql, ex.getMessage());
            throw new RuntimeException("Unable to get composite type fields", ex);
        }
    }

    public void dropType(String typeName) {
        validateIdentifier(typeName, "type");
        String sql = "DROP TYPE " + quoteIdentifier(typeName);
        try {
            jdbcTemplate.execute(sql);
            logService.logOperation(sql);
        } catch (Exception ex) {
            logService.logError(sql, ex.getMessage());
            throw new RuntimeException("Failed to drop type: " + ex.getMessage(), ex);
        }
    }

    public void addEnumValue(String enumName, String newValue, String position, String refValue) {
        validateIdentifier(enumName, "enum");
        if (newValue == null || newValue.isBlank()) {
            throw new IllegalArgumentException("New value must not be blank");
        }
        
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TYPE ").append(quoteIdentifier(enumName));
        sql.append(" ADD VALUE '").append(newValue.replace("'", "''")).append("'");
        
        if (position != null && !"At end".equals(position) && refValue != null && !refValue.isBlank()) {
            if ("Before existing value".equals(position)) {
                sql.append(" BEFORE '").append(refValue.replace("'", "''")).append("'");
            } else if ("After existing value".equals(position)) {
                sql.append(" AFTER '").append(refValue.replace("'", "''")).append("'");
            }
        }
        
        try {
            jdbcTemplate.execute(sql.toString());
            logService.logOperation(sql.toString());
        } catch (Exception ex) {
            logService.logError(sql.toString(), ex.getMessage());
            throw new RuntimeException("Failed to add enum value: " + ex.getMessage(), ex);
        }
    }

    // ========== ADVANCED QUERY WITH SUBQUERIES ==========

    public List<Map<String, Object>> fetchDataWithSubqueries(String tableName,
                                                              List<String> selectColumns,
                                                              List<String> computedColumns,
                                                              List<DataFilter> filters,
                                                              List<String> subqueryFilters,
                                                              List<String> orderByColumns,
                                                              List<String> orderDirections,
                                                              List<String> groupByColumns,
                                                              List<String> aggregates,
                                                              String havingClause) {
        validateIdentifier(tableName, "table");
        
        // Build SELECT clause
        StringBuilder sql = new StringBuilder("SELECT ");
        List<String> selectParts = new ArrayList<>();
        
        if (selectColumns == null || selectColumns.isEmpty()) {
            selectParts.add("*");
        } else {
            for (String col : selectColumns) {
                if (col.contains("(") || col.contains(" AS ")) {
                    selectParts.add(col);
                } else {
                    selectParts.add(quoteIdentifier(col));
                }
            }
        }
        
        // Add computed columns (CASE expressions)
        if (computedColumns != null) {
            selectParts.addAll(computedColumns);
        }
        
        // Add aggregates if present
        if (aggregates != null && !aggregates.isEmpty()) {
            selectParts.addAll(aggregates);
        }
        
        sql.append(String.join(", ", selectParts));
        sql.append(" FROM ").append(quoteIdentifier(tableName));
        
        // Build WHERE clause
        MapSqlParameterSource params = new MapSqlParameterSource();
        List<String> whereClauses = new ArrayList<>();
        
        List<DataFilter> activeFilters = filters == null ? List.of() : filters;
        for (int i = 0; i < activeFilters.size(); i++) {
            DataFilter filter = activeFilters.get(i);
            String clause = buildFilterClause(filter, i, params);
            if (clause != null) {
                whereClauses.add(clause);
            }
        }
        
        // Add subquery filters
        if (subqueryFilters != null) {
            whereClauses.addAll(subqueryFilters);
        }
        
        if (!whereClauses.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", whereClauses));
        }
        
        // Build GROUP BY clause
        if (groupByColumns != null && !groupByColumns.isEmpty()) {
            sql.append(" GROUP BY ");
            List<String> quoted = groupByColumns.stream().map(this::quoteIdentifier).toList();
            sql.append(String.join(", ", quoted));
        }
        
        // Build HAVING clause
        if (havingClause != null && !havingClause.isBlank()) {
            sql.append(" HAVING ").append(havingClause);
        }
        
        // Build ORDER BY clause
        if (orderByColumns != null && !orderByColumns.isEmpty()) {
            sql.append(" ORDER BY ");
            List<String> orderClauses = new ArrayList<>();
            for (int i = 0; i < orderByColumns.size(); i++) {
                String column = quoteIdentifier(orderByColumns.get(i));
                String direction = (orderDirections != null && i < orderDirections.size()) 
                    ? orderDirections.get(i) : "ASC";
                orderClauses.add(column + " " + direction);
            }
            sql.append(String.join(", ", orderClauses));
        }
        
        String logMessage = sql + (!params.getValues().isEmpty() ? " :: " + params.getValues() : "");
        logService.logOperation(logMessage);
        
        try {
            return namedTemplate.queryForList(sql.toString(), params);
        } catch (Exception ex) {
            logService.logError(sql.toString(), ex.getMessage());
            throw ex;
        }
    }
}
