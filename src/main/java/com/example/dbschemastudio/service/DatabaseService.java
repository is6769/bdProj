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
                String paramName = "value" + i;
                clauses.add(quoteIdentifier(filter.column()) + " " + filter.operator() + " :" + paramName);
                params.addValue(paramName, normalizeValue(filter.value(), Optional.of(filter.column())));
            }
            sql.append(" WHERE ").append(String.join(" AND ", clauses));
        }
        String logMessage = sql + (activeFilters.isEmpty() ? "" : " :: " + params.getValues());
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
}
