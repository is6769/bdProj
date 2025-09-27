package com.example.dbschemastudio.controller;

import com.example.dbschemastudio.model.ColumnDefinition;
import com.example.dbschemastudio.model.ColumnMetadata;
import com.example.dbschemastudio.model.ConnectionSettings;
import com.example.dbschemastudio.model.DataFilter;
import com.example.dbschemastudio.service.DatabaseService;
import com.example.dbschemastudio.service.SqlLogService;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.*;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import jakarta.annotation.PreDestroy;

@Component
public class MainViewController {

    private static final List<String> OPERATORS = List.of("=", "!=", ">", "<", ">=", "<=", "LIKE", "ILIKE");

    private final DatabaseService databaseService;
    private final SqlLogService logService;
    private final ConnectionSettings connectionSettings;
    private final ExecutorService dbExecutor;

    @FXML
    private Label statusLabel;
    @FXML
    private Label connectionStatusLabel;
    @FXML
    private Label connectionDetailsLabel;
    @FXML
    private Label transactionStatusLabel;
    @FXML
    private ListView<String> tableListView;
    @FXML
    private TextField tableNameField;
    @FXML
    private TextArea columnDefinitionArea;
    @FXML
    private ComboBox<String> dataTableSelector;
    @FXML
    private VBox filterRowsContainer;
    @FXML
    private TableView<Map<String, Object>> dataTableView;
    @FXML
    private ComboBox<String> insertTableSelector;
    @FXML
    private GridPane insertFormGrid;
    @FXML
    private CheckBox useTransactionCheckBox;
    @FXML
    private Button beginTransactionButton;
    @FXML
    private Button commitTransactionButton;
    @FXML
    private Button rollbackTransactionButton;
    @FXML
    private Button insertRowButton;
    @FXML
    private ListView<String> sqlLogListView;

    private final ObservableList<String> tables = FXCollections.observableArrayList();
    private final List<FilterRow> filterRows = new ArrayList<>();
    private List<String> availableFilterColumns = List.of();
    private final Map<String, TextField> insertFieldMap = new LinkedHashMap<>();
    private List<ColumnMetadata> insertColumnMetadata = List.of();
    public MainViewController(DatabaseService databaseService,
                              SqlLogService logService,
                              ConnectionSettings connectionSettings) {
        this.databaseService = databaseService;
        this.logService = logService;
        this.connectionSettings = connectionSettings;
        this.dbExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "db-worker");
            thread.setDaemon(true);
            return thread;
        });
    }

    @PreDestroy
    public void shutdown() {
        dbExecutor.shutdownNow();
    }

    @FXML
    public void initialize() {
        sqlLogListView.setItems(logService.getEntries());
        tableListView.setItems(tables);
        dataTableSelector.setItems(tables);
        insertTableSelector.setItems(tables);
        initializeFilterRows();

    if (connectionDetailsLabel != null) {
        connectionDetailsLabel.setText(String.format("(%s:%d/%s — schema %s)",
            connectionSettings.host(),
            connectionSettings.port(),
            connectionSettings.database(),
            connectionSettings.schema()));
    }

        tableListView.getSelectionModel().selectedItemProperty().addListener((obs, oldValue, newValue) -> {
            if (newValue != null) {
                dataTableSelector.getSelectionModel().select(newValue);
                insertTableSelector.getSelectionModel().select(newValue);
            }
        });

        updateTransactionStatus();
        handleRefreshConnection();
        handleRefreshTables();
    }

    @FXML
    public void handleAddFilter() {
        addFilterRow(true);
    }

    @FXML
    public void handleRefreshConnection() {
        runAsyncWithErrors("Checking connection", databaseService::testConnection, result -> {
            connectionStatusLabel.setText(result ? "Connected" : "Failed");
            if (!result) {
                setStatus("Unable to reach database. Check configuration.");
            } else {
                setStatus("Connection OK");
            }
        });
    }

    @FXML
    public void handleRefreshTables() {
        runAsyncWithErrors("Loading tables", () -> databaseService.listTables(), loadedTables -> {
            String previousDataSelection = dataTableSelector.getValue();
            String previousInsertSelection = insertTableSelector.getValue();
            String previousListSelection = tableListView.getSelectionModel().getSelectedItem();

            tables.setAll(loadedTables);

            if (previousListSelection != null && loadedTables.contains(previousListSelection)) {
                tableListView.getSelectionModel().select(previousListSelection);
            } else if (!loadedTables.isEmpty()) {
                tableListView.getSelectionModel().select(loadedTables.get(0));
            } else {
                tableListView.getSelectionModel().clearSelection();
            }

            if (previousDataSelection != null && loadedTables.contains(previousDataSelection)) {
                dataTableSelector.getSelectionModel().select(previousDataSelection);
            } else if (!loadedTables.isEmpty()) {
                dataTableSelector.getSelectionModel().select(loadedTables.get(0));
            } else {
                dataTableSelector.getSelectionModel().clearSelection();
            }

            if (previousInsertSelection != null && loadedTables.contains(previousInsertSelection)) {
                insertTableSelector.getSelectionModel().select(previousInsertSelection);
            } else if (!loadedTables.isEmpty()) {
                insertTableSelector.getSelectionModel().select(loadedTables.get(0));
            } else {
                insertTableSelector.getSelectionModel().clearSelection();
                insertColumnMetadata = List.of();
                insertFormGrid.getChildren().clear();
                if (insertRowButton != null) {
                    insertRowButton.setDisable(true);
                }
            }

            setStatus("Loaded " + loadedTables.size() + " table(s)");
        });
    }

    @FXML
    public void handleCreateTable() {
        String tableName = Optional.ofNullable(tableNameField.getText()).map(String::trim).orElse("");
        String columnText = Optional.ofNullable(columnDefinitionArea.getText()).orElse("");
        if (tableName.isBlank() || columnText.isBlank()) {
            showError("Please provide a table name and at least one column definition.");
            return;
        }
        List<ColumnDefinition> definitions;
        try {
            definitions = parseColumnDefinitions(columnText);
        } catch (IllegalArgumentException ex) {
            showError(ex.getMessage());
            return;
        }
        runAsyncVoid("Creating table", () -> databaseService.createTable(tableName, definitions), () -> {
            setStatus("Table " + tableName + " created (if not existing)");
            tableNameField.clear();
            columnDefinitionArea.clear();
            handleRefreshTables();
        });
    }

    @FXML
    public void handleDataTableSelected() {
        String table = dataTableSelector.getValue();
        if (table == null) {
            return;
        }
        runAsyncWithErrors("Loading columns", () -> databaseService.describeTable(table), columns -> {
            List<String> columnNames = columns.stream().map(ColumnMetadata::name).toList();
            availableFilterColumns = columnNames;
            resetFilters();
            handleReloadData();
        });
    }

    @FXML
    public void handleReloadData() {
        String table = dataTableSelector.getValue();
        if (table == null) {
            showError("Select a table to load data");
            return;
        }
        List<DataFilter> filters;
        try {
            filters = buildFilters();
        } catch (IllegalArgumentException ex) {
            showError(ex.getMessage());
            return;
        }
        runAsyncWithErrors("Fetching data", () -> databaseService.fetchData(table, filters), data -> {
            populateTable(data);
            setStatus("Loaded " + data.size() + " row(s)");
        });
    }

    @FXML
    public void handleClearFilter() {
        resetFilters();
        if (dataTableSelector.getValue() != null) {
            handleReloadData();
        }
    }

    @FXML
    public void handleInsertTableSelected() {
        String table = insertTableSelector.getValue();
        if (table == null) {
            return;
        }
        runAsyncWithErrors("Describing table", () -> databaseService.describeTable(table), columns -> {
            insertColumnMetadata = columns;
            buildInsertForm(columns);
        });
    }

    @FXML
    public void handleResetInsertForm() {
        insertFieldMap.values().forEach(TextField::clear);
    }

    @FXML
    public void handleInsertRow() {
        String table = insertTableSelector.getValue();
        if (table == null) {
            showError("Select a table for insertion");
            return;
        }
        Map<String, Object> values = new LinkedHashMap<>();
        for (ColumnMetadata metadata : insertColumnMetadata) {
            if (metadata.autoIncrement()) {
                continue;
            }
            TextField field = insertFieldMap.get(metadata.name());
            if (field == null) {
                continue;
            }
            String text = field.getText();
            if (!metadata.nullable() && (text == null || text.isBlank())) {
                showError("Column '" + metadata.name() + "' is required");
                return;
            }
            values.put(metadata.name(), text);
        }
        boolean useTransaction = useTransactionCheckBox.isSelected();
        runAsyncVoid("Inserting row", () -> databaseService.insertData(table, values, useTransaction), () -> {
            setStatus("Row inserted into " + table + (useTransaction ? " (pending commit)" : ""));
            refreshDataForTable(table);
            handleResetInsertForm();
        });
    }

    @FXML
    public void handleBeginTransaction() {
        runAsyncVoid("Starting transaction", databaseService::beginTransaction, () -> updateTransactionStatus());
    }

    @FXML
    public void handleCommitTransaction() {
        runAsyncVoid("Committing transaction", databaseService::commitTransaction, () -> refreshDataForActiveTable());
    }

    @FXML
    public void handleRollbackTransaction() {
        runAsyncVoid("Rolling back transaction", databaseService::rollbackTransaction, this::refreshDataForActiveTable);
    }

    private void updateTransactionStatus() {
        boolean active = databaseService.isTransactionActive();
        transactionStatusLabel.setText(active ? "Active" : "Inactive");
        if (beginTransactionButton != null) {
            beginTransactionButton.setDisable(active);
        }
        if (commitTransactionButton != null) {
            commitTransactionButton.setDisable(!active);
        }
        if (rollbackTransactionButton != null) {
            rollbackTransactionButton.setDisable(!active);
        }
    }

    private List<DataFilter> buildFilters() {
        List<DataFilter> filters = new ArrayList<>();
        for (FilterRow row : filterRows) {
            String column = row.columnCombo.getValue();
            String operator = row.operatorCombo.getValue();
            String value = row.valueField.getText();
            boolean hasColumn = column != null && !column.isBlank();
            boolean hasValue = value != null && !value.isBlank();
            if (!hasColumn && !hasValue) {
                continue;
            }
            if (!hasColumn) {
                throw new IllegalArgumentException("Select a column for each filter with a value.");
            }
            if (!hasValue) {
                throw new IllegalArgumentException("Provide a value for filter on column '" + column + "'.");
            }
            filters.add(new DataFilter(column, operator, value));
        }
        return filters;
    }

    private void initializeFilterRows() {
        if (filterRowsContainer == null) {
            return;
        }
        resetFilters();
    }

    private FilterRow addFilterRow(boolean focusValue) {
        ComboBox<String> columnCombo = new ComboBox<>();
        columnCombo.setPrefWidth(150);
        columnCombo.setPromptText("Column");

        ComboBox<String> operatorCombo = new ComboBox<>();
        operatorCombo.setPrefWidth(80);
        operatorCombo.setItems(FXCollections.observableArrayList(OPERATORS));
        operatorCombo.getSelectionModel().selectFirst();

        TextField valueField = new TextField();
        valueField.setPromptText("Value");

        Button removeButton = new Button("Remove");

        HBox rowContainer = new HBox(10, columnCombo, operatorCombo, valueField, removeButton);
        rowContainer.setAlignment(Pos.CENTER_LEFT);

        FilterRow row = new FilterRow(rowContainer, columnCombo, operatorCombo, valueField, removeButton);
        removeButton.setOnAction(event -> removeFilterRow(row));

        filterRows.add(row);
        filterRowsContainer.getChildren().add(rowContainer);
        row.setAvailableColumns(availableFilterColumns);
        updateFilterRowState();

        if (focusValue) {
            Platform.runLater(valueField::requestFocus);
        }
        return row;
    }

    private void removeFilterRow(FilterRow row) {
        if (filterRows.size() <= 1) {
            row.clear();
            return;
        }
        filterRows.remove(row);
        filterRowsContainer.getChildren().remove(row.container);
        updateFilterRowState();
    }

    private void resetFilters() {
        filterRowsContainer.getChildren().clear();
        filterRows.clear();
        FilterRow row = addFilterRow(false);
        row.clear();
        syncFilterRowColumns();
    }

    private void updateFilterRowState() {
        boolean disableRemove = filterRows.size() <= 1;
        for (FilterRow row : filterRows) {
            row.removeButton.setDisable(disableRemove);
        }
    }

    private void syncFilterRowColumns() {
        for (FilterRow row : filterRows) {
            row.setAvailableColumns(availableFilterColumns);
        }
    }

    private void populateTable(List<Map<String, Object>> data) {
        dataTableView.getColumns().clear();
        if (data.isEmpty()) {
            dataTableView.setItems(FXCollections.observableArrayList());
            return;
        }
        Map<String, Object> firstRow = data.get(0);
        for (String key : firstRow.keySet()) {
            TableColumn<Map<String, Object>, Object> column = new TableColumn<>(key);
            column.setCellValueFactory(param -> new ReadOnlyObjectWrapper<>(param.getValue().get(key)));
            column.setPrefWidth(120);
            dataTableView.getColumns().add(column);
        }
        ObservableList<Map<String, Object>> rows = FXCollections.observableArrayList(data);
        dataTableView.setItems(rows);
    }

    private void buildInsertForm(List<ColumnMetadata> columns) {
        insertFormGrid.getChildren().clear();
        insertFieldMap.clear();
        insertFormGrid.setHgap(10);
        insertFormGrid.setVgap(10);
        insertFormGrid.setPadding(new Insets(4));
        int row = 0;
        for (ColumnMetadata column : columns) {
            Label label = new Label(column.name() + " (" + column.typeName() + ")");
            insertFormGrid.add(label, 0, row);
            if (column.autoIncrement()) {
                Label generated = new Label("Auto-generated");
                generated.getStyleClass().add("text-muted");
                insertFormGrid.add(generated, 1, row);
            } else {
                TextField field = new TextField();
                if (!column.nullable()) {
                    field.setPromptText("required");
                }
                insertFormGrid.add(field, 1, row);
                insertFieldMap.put(column.name(), field);
            }
            row++;
        }
        if (insertRowButton != null) {
            insertRowButton.setDisable(columns.isEmpty());
        }
    }

    private List<ColumnDefinition> parseColumnDefinitions(String text) {
        String[] lines = text.split("\r?\n");
        List<ColumnDefinition> definitions = new ArrayList<>();
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String[] parts = trimmed.split("\\s+", 2);
            if (parts.length < 2) {
                throw new IllegalArgumentException("Invalid column definition: " + line);
            }
            definitions.add(new ColumnDefinition(parts[0], parts[1]));
        }
        if (definitions.isEmpty()) {
            throw new IllegalArgumentException("Provide at least one valid column definition");
        }
        return definitions;
    }

    private void setStatus(String message) {
        statusLabel.setText(message);
    }

    private void showError(String message) {
        statusLabel.setText(message);
        logService.logError("<ui>", message);
    }

    private <T> void runAsyncWithErrors(String label, Supplier<T> task, Consumer<T> onSuccess) {
        statusLabel.setText(label + "...");
        CompletableFuture
                .supplyAsync(() -> {
                    try {
                        return task.get();
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                }, dbExecutor)
                .whenComplete((result, throwable) -> Platform.runLater(() -> {
                    if (throwable != null) {
                        handleError(label, throwable);
                    } else {
                        onSuccess.accept(result);
                    }
                }));
    }

    private void runAsyncVoid(String label, Runnable runnable, Runnable onSuccess) {
        statusLabel.setText(label + "...");
        CompletableFuture
                .runAsync(() -> {
                    try {
                        runnable.run();
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                }, dbExecutor)
                .whenComplete((ignored, throwable) -> Platform.runLater(() -> {
                    if (throwable != null) {
                        handleError(label, throwable);
                    } else {
                        onSuccess.run();
                    }
                }));
    }

    private void handleError(String label, Throwable throwable) {
        Throwable cause = throwable instanceof CompletionException && throwable.getCause() != null
                ? throwable.getCause() : throwable;
        String message = cause.getMessage() != null ? cause.getMessage() : cause.toString();
        statusLabel.setText(label + " failed: " + message);
        logService.logError(label, message);
    }

    private void refreshDataForTable(String table) {
        if (table == null) {
            return;
        }
        String current = dataTableSelector.getValue();
        if (!Objects.equals(current, table)) {
            dataTableSelector.getSelectionModel().select(table);
            handleDataTableSelected();
        } else {
            handleReloadData();
        }
    }

    private void refreshDataForActiveTable() {
        String table = dataTableSelector.getValue();
        updateTransactionStatus();
        if (table != null) {
            handleReloadData();
        }
    }

    private static class FilterRow {
        private final HBox container;
        private final ComboBox<String> columnCombo;
        private final ComboBox<String> operatorCombo;
        private final TextField valueField;
        private final Button removeButton;

        private FilterRow(HBox container,
                           ComboBox<String> columnCombo,
                           ComboBox<String> operatorCombo,
                           TextField valueField,
                           Button removeButton) {
            this.container = container;
            this.columnCombo = columnCombo;
            this.operatorCombo = operatorCombo;
            this.valueField = valueField;
            this.removeButton = removeButton;
        }

        private void setAvailableColumns(List<String> columns) {
            String previous = columnCombo.getValue();
            columnCombo.setItems(FXCollections.observableArrayList(columns));
            if (previous != null && columns.contains(previous)) {
                columnCombo.setValue(previous);
            } else {
                columnCombo.getSelectionModel().clearSelection();
            }
        }

        private void clear() {
            columnCombo.getSelectionModel().clearSelection();
            operatorCombo.getSelectionModel().selectFirst();
            valueField.clear();
        }
    }
}
