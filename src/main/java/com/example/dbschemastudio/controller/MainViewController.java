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

    private static final List<String> OPERATORS = List.of(
        "=", "!=", ">", "<", ">=", "<=",
        "LIKE", "ILIKE", "NOT LIKE", "NOT ILIKE",
        "~ (regex)", "~* (regex ci)", "!~ (not regex)", "!~* (not regex ci)",
        "SIMILAR TO", "NOT SIMILAR TO",
        "IS NULL", "IS NOT NULL",
        "ANY (subquery)", "ALL (subquery)", "EXISTS (subquery)"
    );
    private static final List<String> COMMON_TYPES = List.of(
        "SERIAL", "BIGSERIAL", "INTEGER", "BIGINT", "SMALLINT",
        "VARCHAR", "TEXT", "CHAR",
        "BOOLEAN",
        "NUMERIC", "DECIMAL", "REAL", "DOUBLE PRECISION",
        "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ",
        "UUID", "JSON", "JSONB"
    );

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
    private TextField enumNameField;
    @FXML
    private TextField enumValuesField;
    @FXML
    private ListView<String> enumListView;
    @FXML
    private VBox columnRowsContainer;
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

    // Alter Table tab controls
    @FXML
    private ComboBox<String> alterTableSelector;
    @FXML
    private TextField newTableNameField;
    @FXML
    private TextField addColumnNameField;
    @FXML
    private ComboBox<String> addColumnTypeCombo;
    @FXML
    private TextField addColumnConstraintsField;
    @FXML
    private ComboBox<String> dropColumnSelector;
    @FXML
    private ComboBox<String> renameColumnSelector;
    @FXML
    private TextField newColumnNameField;
    @FXML
    private ComboBox<String> alterColumnTypeSelector;
    @FXML
    private ComboBox<String> newColumnTypeCombo;
    @FXML
    private CheckBox usingCastCheckbox;
    @FXML
    private ComboBox<String> constraintTypeCombo;
    @FXML
    private TextField constraintNameField;
    @FXML
    private TextField constraintDefinitionField;
    @FXML
    private TextField dropConstraintNameField;
    @FXML
    private ComboBox<String> notNullColumnSelector;

    // Advanced SELECT controls
    @FXML
    private CheckBox selectAllColumnsCheckbox;
    @FXML
    private Label selectedColumnsLabel;
    @FXML
    private ComboBox<String> orderByColumnCombo;
    @FXML
    private ComboBox<String> orderDirectionCombo;
    @FXML
    private VBox sortRowsContainer;
    @FXML
    private CheckBox enableGroupByCheckbox;
    @FXML
    private ComboBox<String> groupByColumnCombo;
    @FXML
    private Button addGroupButton;
    @FXML
    private Button clearGroupButton;
    @FXML
    private VBox groupByRowsContainer;
    @FXML
    private VBox aggregateSection;
    @FXML
    private ComboBox<String> aggregateFunctionCombo;
    @FXML
    private ComboBox<String> aggregateColumnCombo;
    @FXML
    private TextField aggregateAliasField;
    @FXML
    private VBox aggregateRowsContainer;
    @FXML
    private VBox havingSection;
    @FXML
    private TextField havingConditionField;

    // JOIN Wizard controls
    @FXML
    private ComboBox<String> joinLeftTableCombo;
    @FXML
    private TextField joinLeftAliasField;
    @FXML
    private VBox joinRowsContainer;
    @FXML
    private Label joinSelectedColumnsLabel;
    @FXML
    private TextField joinWhereField;
    @FXML
    private TextField joinOrderByField;
    @FXML
    private TextField joinLimitField;
    @FXML
    private TableView<Map<String, Object>> joinResultsTableView;

    // User-defined types controls
    @FXML
    private TextField compositeTypeNameField;
    @FXML
    private VBox compositeFieldsContainer;
    @FXML
    private ListView<String> compositeTypeListView;

    // Computed columns/expressions
    private final List<ComputedColumn> computedColumns = new ArrayList<>();
    private final List<SubqueryFilter> subqueryFilters = new ArrayList<>();

    private final ObservableList<String> tables = FXCollections.observableArrayList();
    private final ObservableList<String> enums = FXCollections.observableArrayList();
    private final List<FilterRow> filterRows = new ArrayList<>();
    private final List<ColumnRow> columnRows = new ArrayList<>();
    private final List<SortRow> sortRows = new ArrayList<>();
    private final List<String> groupByColumns = new ArrayList<>();
    private final List<AggregateFunction> aggregateFunctions = new ArrayList<>();
    private final List<JoinRow> joinRows = new ArrayList<>();
    private List<String> selectedColumns = new ArrayList<>();
    private List<String> joinSelectedColumns = new ArrayList<>();
    private List<String> availableFilterColumns = List.of();
    private List<String> availableTypes = new ArrayList<>(COMMON_TYPES);
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
        enumListView.setItems(enums);
        dataTableSelector.setItems(tables);
        insertTableSelector.setItems(tables);
        
        // Initialize Alter Table tab
        if (alterTableSelector != null) {
            alterTableSelector.setItems(tables);
            initializeAlterTableTab();
        }
        
        // Initialize Advanced SELECT controls
        initializeAdvancedSelectControls();
        
        // Initialize JOIN Wizard
        if (joinLeftTableCombo != null) {
            joinLeftTableCombo.setItems(tables);
        }
        
        initializeFilterRows();
        initializeColumnRows();

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
                if (alterTableSelector != null) {
                    alterTableSelector.getSelectionModel().select(newValue);
                }
            }
        });
        
        enumListView.setOnMouseClicked(event -> {
            if (event.getClickCount() == 2) {
                String selected = enumListView.getSelectionModel().getSelectedItem();
                if (selected != null) {
                    showEnumValues(selected);
                }
            }
        });

        updateTransactionStatus();
        handleRefreshConnection();
        handleRefreshTables();
        handleRefreshEnums();
    }

    private void initializeAlterTableTab() {
        // Populate constraint types
        if (constraintTypeCombo != null) {
            constraintTypeCombo.setItems(FXCollections.observableArrayList(
                "CHECK", "UNIQUE", "PRIMARY KEY", "FOREIGN KEY"
            ));
        }
        
        // Populate type combos with common types + enums
        if (addColumnTypeCombo != null) {
            addColumnTypeCombo.setItems(FXCollections.observableArrayList(availableTypes));
        }
        if (newColumnTypeCombo != null) {
            newColumnTypeCombo.setItems(FXCollections.observableArrayList(availableTypes));
        }
    }

    private void initializeAdvancedSelectControls() {
        // Initialize sort direction combo
        if (orderDirectionCombo != null) {
            orderDirectionCombo.setItems(FXCollections.observableArrayList("ASC", "DESC"));
            orderDirectionCombo.setValue("ASC");
        }
        
        // Initialize aggregate function combo
        if (aggregateFunctionCombo != null) {
            aggregateFunctionCombo.setItems(FXCollections.observableArrayList(
                "COUNT", "SUM", "AVG", "MIN", "MAX", "COUNT(*)"
            ));
        }
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
        if (tableName.isBlank()) {
            showError("Please provide a table name.");
            return;
        }
        List<ColumnDefinition> definitions;
        try {
            definitions = buildColumnDefinitions();
        } catch (IllegalArgumentException ex) {
            showError(ex.getMessage());
            return;
        }
        if (definitions.isEmpty()) {
            showError("Please add at least one column.");
            return;
        }
        runAsyncVoid("Creating table", () -> databaseService.createTable(tableName, definitions), () -> {
            setStatus("Table " + tableName + " created successfully");
            handleClearTableForm();
            handleRefreshTables();
        });
    }

    @FXML
    public void handleCreateEnum() {
        String enumName = Optional.ofNullable(enumNameField.getText()).map(String::trim).orElse("");
        String valuesText = Optional.ofNullable(enumValuesField.getText()).map(String::trim).orElse("");
        if (enumName.isBlank() || valuesText.isBlank()) {
            showError("Please provide enum name and values.");
            return;
        }
        List<String> values = Arrays.stream(valuesText.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
        if (values.isEmpty()) {
            showError("Please provide at least one enum value.");
            return;
        }
        runAsyncVoid("Creating enum", () -> databaseService.createEnum(enumName, values), () -> {
            setStatus("Enum type " + enumName + " created successfully");
            enumNameField.clear();
            enumValuesField.clear();
            handleRefreshEnums();
        });
    }

    @FXML
    public void handleRefreshEnums() {
        runAsyncWithErrors("Loading enums", () -> databaseService.listEnums(), loadedEnums -> {
            enums.setAll(loadedEnums);
            availableTypes = new ArrayList<>(COMMON_TYPES);
            availableTypes.addAll(loadedEnums);
            syncColumnRowTypes();
            setStatus("Loaded " + loadedEnums.size() + " enum type(s)");
        });
    }

    @FXML
    public void handleAddColumn() {
        addColumnRow();
    }

    @FXML
    public void handleClearTableForm() {
        tableNameField.clear();
        columnRowsContainer.getChildren().clear();
        columnRows.clear();
        addColumnRow();
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
            
            // Update sorting combos
            if (orderByColumnCombo != null) {
                orderByColumnCombo.setItems(FXCollections.observableArrayList(columnNames));
            }
            
            // Update grouping combos
            if (groupByColumnCombo != null) {
                groupByColumnCombo.setItems(FXCollections.observableArrayList(columnNames));
            }
            
            // Update aggregate column combo
            if (aggregateColumnCombo != null) {
                aggregateColumnCombo.setItems(FXCollections.observableArrayList(columnNames));
            }
            
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
        // Check if advanced SELECT features are being used
        boolean hasSort = !sortRows.isEmpty();
        boolean hasGroup = enableGroupByCheckbox != null && enableGroupByCheckbox.isSelected();
        boolean hasAggregates = !aggregateFunctions.isEmpty();
        boolean hasSubqueryFilters = !subqueryFilters.isEmpty();
        boolean hasComputedColumns = !computedColumns.isEmpty();
        boolean useAdvanced = hasSort || hasGroup || hasAggregates || selectedColumns != null 
                              || hasSubqueryFilters || hasComputedColumns;
        
        if (!useAdvanced) {
            // Use simple query
            runAsyncWithErrors("Fetching data", () -> databaseService.fetchData(table, filters), data -> {
                populateTable(data);
                setStatus("Loaded " + data.size() + " row(s)");
            });
        } else {
            // Use advanced query with subqueries and computed columns
            List<String> selectCols = (selectAllColumnsCheckbox != null && selectAllColumnsCheckbox.isSelected()) 
                ? null : selectedColumns;
            
            List<String> orderCols = sortRows.stream().map(row -> row.column).toList();
            List<String> orderDirs = sortRows.stream().map(row -> row.direction).toList();
            
            List<String> aggList = aggregateFunctions.stream().map(AggregateFunction::toSQL).toList();
            
            // Build computed columns SQL (CASE expressions)
            List<String> computedColsSQL = computedColumns.stream().map(ComputedColumn::toSQL).toList();
            
            // Build subquery filter SQL
            List<String> subqueryFilterSQL = subqueryFilters.stream()
                .map(f -> f.toSQL(table))
                .toList();
            
            String having = (havingConditionField != null && havingConditionField.getText() != null) 
                ? havingConditionField.getText().trim() : "";
            if (having.isBlank()) {
                having = null;
            }
            
            List<String> finalSelectCols = selectCols;
            List<String> finalOrderCols = orderCols;
            List<String> finalOrderDirs = orderDirs;
            List<String> finalAggList = aggList;
            List<String> finalComputedCols = computedColsSQL;
            List<String> finalSubqueryFilters = subqueryFilterSQL;
            String finalHaving = having;
            
            runAsyncWithErrors("Fetching data", 
                () -> databaseService.fetchDataWithSubqueries(table, finalSelectCols, finalComputedCols, filters, 
                    finalSubqueryFilters, finalOrderCols, finalOrderDirs, groupByColumns, finalAggList, finalHaving), 
                data -> {
                    populateTable(data);
                    setStatus("Loaded " + data.size() + " row(s)");
                });
        }
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
            
            // Skip completely empty rows
            if (!hasColumn && !hasValue ){//&& (operator == null || operator.isBlank())) {
                continue;
            }
            
            if (!hasColumn) {
                throw new IllegalArgumentException("Select a column for each filter.");
            }
            
            // Clean operator - remove descriptions in parentheses
            String cleanOperator = operator;
            if (operator != null && operator.contains("(")) {
                cleanOperator = operator.substring(0, operator.indexOf("(")).trim();
            }
            
            // IS NULL and IS NOT NULL don't require a value
            boolean isNullOperator = "IS NULL".equals(cleanOperator) || "IS NOT NULL".equals(cleanOperator);
            
            // Skip subquery operators in regular filters (they're handled separately)
            boolean isSubqueryOperator = cleanOperator != null && 
                (cleanOperator.startsWith("ANY") || cleanOperator.startsWith("ALL") || cleanOperator.startsWith("EXISTS"));
            if (isSubqueryOperator) {
                continue;
            }
            
            if (!isNullOperator && !hasValue) {
                throw new IllegalArgumentException("Provide a value for filter on column '" + column + "'.");
            }
            
            // For IS NULL/IS NOT NULL, use empty value
            String filterValue = isNullOperator ? "" : value;
            
            filters.add(new DataFilter(column, cleanOperator, filterValue));
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

    private void initializeColumnRows() {
        if (columnRowsContainer == null) {
            return;
        }
        columnRowsContainer.getChildren().clear();
        columnRows.clear();
        addColumnRow();
    }

    private void addColumnRow() {
        TextField nameField = new TextField();
        nameField.setPromptText("Column name");
        nameField.setPrefWidth(150);

        ComboBox<String> typeCombo = new ComboBox<>();
        typeCombo.setPrefWidth(150);
        typeCombo.setEditable(true);
        typeCombo.setPromptText("Type");
        typeCombo.setItems(FXCollections.observableArrayList(availableTypes));

        TextField constraintsField = new TextField();
        constraintsField.setPromptText("Constraints (e.g. NOT NULL, PRIMARY KEY)");
        constraintsField.setPrefWidth(250);

        Button removeButton = new Button("Remove");
        
        HBox rowContainer = new HBox(10, nameField, typeCombo, constraintsField, removeButton);
        rowContainer.setAlignment(Pos.CENTER_LEFT);

        ColumnRow row = new ColumnRow(rowContainer, nameField, typeCombo, constraintsField, removeButton);
        removeButton.setOnAction(event -> removeColumnRow(row));

        columnRows.add(row);
        columnRowsContainer.getChildren().add(rowContainer);
        updateColumnRowState();
    }

    private void removeColumnRow(ColumnRow row) {
        if (columnRows.size() <= 1) {
            row.clear();
            return;
        }
        columnRows.remove(row);
        columnRowsContainer.getChildren().remove(row.container);
        updateColumnRowState();
    }

    private void updateColumnRowState() {
        boolean disableRemove = columnRows.size() <= 1;
        for (ColumnRow row : columnRows) {
            row.removeButton.setDisable(disableRemove);
        }
    }

    private void syncColumnRowTypes() {
        for (ColumnRow row : columnRows) {
            row.setAvailableTypes(availableTypes);
        }
    }

    private List<ColumnDefinition> buildColumnDefinitions() {
        List<ColumnDefinition> definitions = new ArrayList<>();
        for (ColumnRow row : columnRows) {
            String name = row.nameField.getText();
            String type = row.typeCombo.getValue();
            String constraints = row.constraintsField.getText();
            
            boolean hasName = name != null && !name.isBlank();
            boolean hasType = type != null && !type.isBlank();
            
            if (!hasName && !hasType) {
                continue;
            }
            if (!hasName) {
                throw new IllegalArgumentException("Column name is required for all columns.");
            }
            if (!hasType) {
                throw new IllegalArgumentException("Type is required for column '" + name + "'.");
            }
            
            String typeClause = type.trim();
            if (constraints != null && !constraints.isBlank()) {
                typeClause += " " + constraints.trim();
            }
            
            definitions.add(new ColumnDefinition(name.trim(), typeClause));
        }
        return definitions;
    }

    private void showEnumValues(String enumName) {
        runAsyncWithErrors("Loading enum values", () -> databaseService.getEnumValues(enumName), values -> {
            String valuesText = String.join(", ", values);
            Alert alert = new Alert(Alert.AlertType.INFORMATION);
            alert.setTitle("ENUM Values");
            alert.setHeaderText("Values for enum type: " + enumName);
            alert.setContentText(valuesText);
            alert.showAndWait();
        });
    }

    /** Parse column definitions from text. Useful for parsing CREATE TABLE text input. */
    @SuppressWarnings("unused")
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

    // ========== ADVANCED SELECT HANDLERS ==========

    @FXML
    private void handleSelectAllColumns() {
        boolean selectAll = selectAllColumnsCheckbox != null && selectAllColumnsCheckbox.isSelected();
        if (selectAll) {
            selectedColumns = new ArrayList<>();
            if (selectedColumnsLabel != null) {
                selectedColumnsLabel.setText("");
            }
        }
    }

    @FXML
    private void handleChooseColumns() {
        String tableName = dataTableSelector.getValue();
        if (tableName == null || tableName.isBlank()) {
            showError("Please select a table first");
            return;
        }
        
        runAsyncWithErrors("Loading columns", () -> databaseService.describeTable(tableName), columns -> {
            List<String> columnNames = columns.stream().map(ColumnMetadata::name).toList();
            showColumnSelectionDialog(columnNames);
        });
    }

    private void showColumnSelectionDialog(List<String> allColumns) {
        Dialog<List<String>> dialog = new Dialog<>();
        dialog.setTitle("Select Columns");
        dialog.setHeaderText("Choose columns to display");
        
        VBox content = new VBox(10);
        List<CheckBox> checkBoxes = new ArrayList<>();
        
        for (String col : allColumns) {
            CheckBox cb = new CheckBox(col);
            cb.setSelected(selectedColumns.isEmpty() || selectedColumns.contains(col));
            checkBoxes.add(cb);
            content.getChildren().add(cb);
        }
        
        ScrollPane scrollPane = new ScrollPane(content);
        scrollPane.setFitToWidth(true);
        scrollPane.setPrefHeight(300);
        
        dialog.getDialogPane().setContent(scrollPane);
        dialog.getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);
        
        dialog.setResultConverter(buttonType -> {
            if (buttonType == ButtonType.OK) {
                return checkBoxes.stream()
                    .filter(CheckBox::isSelected)
                    .map(CheckBox::getText)
                    .toList();
            }
            return null;
        });
        
        dialog.showAndWait().ifPresent(selected -> {
            selectedColumns = new ArrayList<>(selected);
            if (selectAllColumnsCheckbox != null) {
                // selectAllColumnsCheckbox.setSelected(selected.isEmpty());
            }
            if (selectedColumnsLabel != null) {
                selectedColumnsLabel.setText(selected.isEmpty() ?
                        "(No columns selected - using computed/aggregates only)" :
                        "(" + selected.size() + " columns selected)");
            }
        });
    }

    @FXML
    private void handleAddSort() {
        String column = orderByColumnCombo != null ? orderByColumnCombo.getValue() : null;
        String direction = orderDirectionCombo != null ? orderDirectionCombo.getValue() : "ASC";
        
        if (column == null || column.isBlank()) {
            showError("Please select a column to sort by");
            return;
        }
        
        // Create sort row UI
        HBox sortRow = new HBox(10);
        sortRow.setAlignment(Pos.CENTER_LEFT);
        
        Label columnLabel = new Label(column);
        Label directionLabel = new Label(direction);
        Button removeButton = new Button("Remove");
        
        SortRow row = new SortRow(sortRow, column, direction, removeButton);
        sortRows.add(row);
        
        sortRow.getChildren().addAll(columnLabel, new Label("→"), directionLabel, removeButton);
        
        removeButton.setOnAction(e -> {
            sortRows.remove(row);
            if (sortRowsContainer != null) {
                sortRowsContainer.getChildren().remove(sortRow);
            }
        });
        
        if (sortRowsContainer != null) {
            sortRowsContainer.getChildren().add(sortRow);
        }
        
        // Clear selections
        if (orderByColumnCombo != null) {
            orderByColumnCombo.getSelectionModel().clearSelection();
        }
    }

    @FXML
    private void handleClearSort() {
        sortRows.clear();
        if (sortRowsContainer != null) {
            sortRowsContainer.getChildren().clear();
        }
    }

    @FXML
    private void handleToggleGroupBy() {
        boolean enabled = enableGroupByCheckbox != null && enableGroupByCheckbox.isSelected();
        
        if (groupByColumnCombo != null) {
            groupByColumnCombo.setDisable(!enabled);
        }
        if (addGroupButton != null) {
            addGroupButton.setDisable(!enabled);
        }
        if (clearGroupButton != null) {
            clearGroupButton.setDisable(!enabled);
        }
        if (aggregateSection != null) {
            aggregateSection.setVisible(enabled);
            aggregateSection.setManaged(enabled);
        }
        if (havingSection != null) {
            havingSection.setVisible(enabled);
            havingSection.setManaged(enabled);
        }
        
        if (!enabled) {
            groupByColumns.clear();
            aggregateFunctions.clear();
            if (groupByRowsContainer != null) {
                groupByRowsContainer.getChildren().clear();
            }
            if (aggregateRowsContainer != null) {
                aggregateRowsContainer.getChildren().clear();
            }
        }
    }

    @FXML
    private void handleAddGroupBy() {
        String column = groupByColumnCombo != null ? groupByColumnCombo.getValue() : null;
        
        if (column == null || column.isBlank()) {
            showError("Please select a column to group by");
            return;
        }
        
        if (groupByColumns.contains(column)) {
            showError("Column already in GROUP BY");
            return;
        }
        
        groupByColumns.add(column);
        
        HBox groupRow = new HBox(10);
        groupRow.setAlignment(Pos.CENTER_LEFT);
        Label label = new Label(column);
        Button removeButton = new Button("Remove");
        
        groupRow.getChildren().addAll(label, removeButton);
        
        removeButton.setOnAction(e -> {
            groupByColumns.remove(column);
            if (groupByRowsContainer != null) {
                groupByRowsContainer.getChildren().remove(groupRow);
            }
        });
        
        if (groupByRowsContainer != null) {
            groupByRowsContainer.getChildren().add(groupRow);
        }
        
        if (groupByColumnCombo != null) {
            groupByColumnCombo.getSelectionModel().clearSelection();
        }
    }

    @FXML
    private void handleClearGroupBy() {
        groupByColumns.clear();
        if (groupByRowsContainer != null) {
            groupByRowsContainer.getChildren().clear();
        }
    }

    @FXML
    private void handleAddAggregate() {
        String function = aggregateFunctionCombo != null ? aggregateFunctionCombo.getValue() : null;
        String column = aggregateColumnCombo != null ? aggregateColumnCombo.getValue() : null;
        String alias = aggregateAliasField != null ? aggregateAliasField.getText() : "";
        
        if (function == null || function.isBlank()) {
            showError("Please select an aggregate function");
            return;
        }
        
        if (function.equals("COUNT(*)")) {
            column = "*";
        } else if (column == null || column.isBlank()) {
            showError("Please select a column");
            return;
        }
        
        AggregateFunction agg = new AggregateFunction(function.replace("(*)", ""), column, alias);
        aggregateFunctions.add(agg);
        
        HBox aggRow = new HBox(10);
        aggRow.setAlignment(Pos.CENTER_LEFT);
        Label label = new Label(agg.toSQL());
        Button removeButton = new Button("Remove");
        
        aggRow.getChildren().addAll(label, removeButton);
        
        removeButton.setOnAction(e -> {
            aggregateFunctions.remove(agg);
            if (aggregateRowsContainer != null) {
                aggregateRowsContainer.getChildren().remove(aggRow);
            }
        });
        
        if (aggregateRowsContainer != null) {
            aggregateRowsContainer.getChildren().add(aggRow);
        }
        
        // Clear selections
        if (aggregateFunctionCombo != null) {
            aggregateFunctionCombo.getSelectionModel().clearSelection();
        }
        if (aggregateColumnCombo != null) {
            aggregateColumnCombo.getSelectionModel().clearSelection();
        }
        if (aggregateAliasField != null) {
            aggregateAliasField.clear();
        }
    }

    @FXML
    private void handleClearAggregates() {
        aggregateFunctions.clear();
        if (aggregateRowsContainer != null) {
            aggregateRowsContainer.getChildren().clear();
        }
    }

    @FXML
    private void handleResetQueryBuilder() {
        // Reset column selection
        if (selectAllColumnsCheckbox != null) {
            selectAllColumnsCheckbox.setSelected(true);
        }
        selectedColumns = new ArrayList<>();
        if (selectedColumnsLabel != null) {
            selectedColumnsLabel.setText("");
        }
        
        // Reset filters
        handleClearFilter();
        
        // Reset sorting
        handleClearSort();
        
        // Reset grouping
        if (enableGroupByCheckbox != null) {
            enableGroupByCheckbox.setSelected(false);
        }
        handleToggleGroupBy();
        
        // Reset HAVING
        if (havingConditionField != null) {
            havingConditionField.clear();
        }
        
        // Reset subquery filters
        subqueryFilters.clear();
        
        // Reset computed columns (CASE expressions)
        computedColumns.clear();
        
        setStatus("Query builder reset");
    }

    @FXML
    private void handleShowStringFunctions() {
        String tableName = dataTableSelector.getValue();
        if (tableName == null || tableName.isBlank()) {
            showError("Please select a table first");
            return;
        }
        
        runAsyncWithErrors("Loading columns", () -> databaseService.describeTable(tableName), columns -> {
            showStringFunctionsDialog(tableName, columns);
        });
    }

    private void showStringFunctionsDialog(String tableName, List<ColumnMetadata> columns) {
        Dialog<Void> dialog = new Dialog<>();
        dialog.setTitle("String Functions");
        dialog.setHeaderText("Apply string functions to columns");
        
        VBox content = new VBox(15);
        content.setPadding(new Insets(15));
        
        // Function selection
        ComboBox<String> functionCombo = new ComboBox<>();
        functionCombo.setItems(FXCollections.observableArrayList(
            "UPPER", "LOWER", "SUBSTRING", "TRIM", "LTRIM", "RTRIM", 
            "LPAD", "RPAD", "CONCAT", "LENGTH", "REPLACE"
        ));
        functionCombo.setPromptText("Select function");
        functionCombo.setPrefWidth(200);
        
        // Column selection
        ComboBox<String> columnCombo = new ComboBox<>();
        columnCombo.setItems(FXCollections.observableArrayList(
            columns.stream().map(ColumnMetadata::name).toList()
        ));
        columnCombo.setPromptText("Select column");
        columnCombo.setPrefWidth(200);
        
        // Parameters (visible conditionally)
        VBox paramsBox = new VBox(10);
        
        TextField param1Field = new TextField();
        param1Field.setPromptText("Parameter 1");
        param1Field.setPrefWidth(200);
        
        TextField param2Field = new TextField();
        param2Field.setPromptText("Parameter 2");
        param2Field.setPrefWidth(200);
        
        TextField param3Field = new TextField();
        param3Field.setPromptText("Parameter 3");
        param3Field.setPrefWidth(200);
        
        Label param1Label = new Label();
        Label param2Label = new Label();
        
        // Update parameters based on function
        functionCombo.setOnAction(e -> {
            String func = functionCombo.getValue();
            paramsBox.getChildren().clear();
            
            if (func == null) return;
            
            switch (func) {
                case "SUBSTRING":
                    param1Label.setText("Start position (1-based):");
                    param2Label.setText("Length:");
                    paramsBox.getChildren().addAll(param1Label, param1Field, param2Label, param2Field);
                    break;
                case "LPAD":
                case "RPAD":
                    param1Label.setText("Total length:");
                    param2Label.setText("Fill character:");
                    paramsBox.getChildren().addAll(param1Label, param1Field, param2Label, param2Field);
                    break;
                case "CONCAT":
                    param1Label.setText("Concatenate with:");
                    paramsBox.getChildren().addAll(param1Label, param1Field);
                    break;
                case "REPLACE":
                    param1Label.setText("Find:");
                    param2Label.setText("Replace with:");
                    paramsBox.getChildren().addAll(param1Label, param1Field, param2Label, param2Field);
                    break;
            }
        });
        
        Button applyButton = new Button("Apply & Show Results");
        TextArea resultArea = new TextArea();
        resultArea.setEditable(false);
        resultArea.setPrefRowCount(10);
        resultArea.setPromptText("Results will appear here...");
        
        applyButton.setOnAction(e -> {
            String function = functionCombo.getValue();
            String column = columnCombo.getValue();
            
            if (function == null || column == null) {
                showError("Please select function and column");
                return;
            }
            
            try {
                String sqlExpression = buildStringFunctionSQL(function, column, 
                    param1Field.getText(), param2Field.getText(), param3Field.getText());
                
                // Execute query to show results
                String query = "SELECT " + quoteIdent(column) + " AS original, " + 
                              sqlExpression + " AS result FROM " + quoteIdent(tableName) + " LIMIT 20";
                
                runAsyncWithErrors("Applying function", 
                    () -> databaseService.executeCustomQuery(query),
                    results -> {
                        StringBuilder sb = new StringBuilder();
                        sb.append("Function: ").append(sqlExpression).append("\n\n");
                        sb.append(String.format("%-30s | %-30s\n", "Original", "Result"));
                        sb.append("-".repeat(63)).append("\n");
                        
                        for (Map<String, Object> row : results) {
                            Object orig = row.get("original");
                            Object res = row.get("result");
                            sb.append(String.format("%-30s | %-30s\n", 
                                orig != null ? orig.toString() : "NULL",
                                res != null ? res.toString() : "NULL"));
                        }
                        
                        resultArea.setText(sb.toString());
                    });
            } catch (Exception ex) {
                showError("Error: " + ex.getMessage());
            }
        });
        
        content.getChildren().addAll(
            new Label("Function:"), functionCombo,
            new Label("Column:"), columnCombo,
            paramsBox,
            applyButton,
            new Separator(),
            new Label("Preview (first 20 rows):"),
            resultArea
        );
        
        ScrollPane scrollPane = new ScrollPane(content);
        scrollPane.setFitToWidth(true);
        scrollPane.setPrefHeight(500);
        scrollPane.setPrefWidth(600);
        
        dialog.getDialogPane().setContent(scrollPane);
        dialog.getDialogPane().getButtonTypes().add(ButtonType.CLOSE);
        
        dialog.showAndWait();
    }

    private String buildStringFunctionSQL(String function, String column, String param1, String param2, String param3) {
        String col = quoteIdent(column);
        
        return switch (function) {
            case "UPPER" -> "UPPER(" + col + ")";
            case "LOWER" -> "LOWER(" + col + ")";
            case "TRIM" -> "TRIM(" + col + ")";
            case "LTRIM" -> "LTRIM(" + col + ")";
            case "RTRIM" -> "RTRIM(" + col + ")";
            case "LENGTH" -> "LENGTH(" + col + ")";
            case "SUBSTRING" -> {
                if (param1 == null || param1.isBlank()) {
                    throw new IllegalArgumentException("SUBSTRING requires start position");
                }
                if (param2 != null && !param2.isBlank()) {
                    yield "SUBSTRING(" + col + " FROM " + param1 + " FOR " + param2 + ")";
                } else {
                    yield "SUBSTRING(" + col + " FROM " + param1 + ")";
                }
            }
            case "LPAD" -> {
                if (param1 == null || param1.isBlank()) {
                    throw new IllegalArgumentException("LPAD requires total length");
                }
                String fill = (param2 != null && !param2.isBlank()) ? "'" + param2 + "'" : "' '";
                yield "LPAD(" + col + ", " + param1 + ", " + fill + ")";
            }
            case "RPAD" -> {
                if (param1 == null || param1.isBlank()) {
                    throw new IllegalArgumentException("RPAD requires total length");
                }
                String fill = (param2 != null && !param2.isBlank()) ? "'" + param2 + "'" : "' '";
                yield "RPAD(" + col + ", " + param1 + ", " + fill + ")";
            }
            case "CONCAT" -> {
                if (param1 == null || param1.isBlank()) {
                    throw new IllegalArgumentException("CONCAT requires a value to concatenate");
                }
                yield col + " || '" + param1 + "'";
            }
            case "REPLACE" -> {
                if (param1 == null || param1.isBlank() || param2 == null) {
                    throw new IllegalArgumentException("REPLACE requires find and replace strings");
                }
                yield "REPLACE(" + col + ", '" + param1 + "', '" + param2 + "')";
            }
            default -> throw new IllegalArgumentException("Unknown function: " + function);
        };
    }

    private String quoteIdent(String identifier) {
        // Simple identifier quoting for PostgreSQL
        return "\"" + identifier + "\"";
    }

    // ========== SUBQUERY FILTER HANDLERS ==========

    @FXML
    private void handleShowSubqueryBuilder() {
        String tableName = dataTableSelector.getValue();
        if (tableName == null || tableName.isBlank()) {
            showError("Please select a table first");
            return;
        }
        
        runAsyncWithErrors("Loading columns", () -> databaseService.describeTable(tableName), columns -> {
            showSubqueryBuilderDialog(tableName, columns);
        });
    }

    private void showSubqueryBuilderDialog(String mainTable, List<ColumnMetadata> mainColumns) {
        Dialog<SubqueryFilter> dialog = new Dialog<>();
        dialog.setTitle("Subquery Filter Builder");
        dialog.setHeaderText("Create a subquery filter (ANY, ALL, EXISTS)");
        
        VBox content = new VBox(15);
        content.setPadding(new Insets(15));
        
        // Operator selection
        Label operatorLabel = new Label("Subquery Operator:");
        ComboBox<String> operatorCombo = new ComboBox<>();
        operatorCombo.setItems(FXCollections.observableArrayList("ANY", "ALL", "EXISTS", "NOT EXISTS", "IN"));
        operatorCombo.setValue("ANY");
        operatorCombo.setPrefWidth(200);
        
        // Main table column (for ANY/ALL)
        Label mainColumnLabel = new Label("Main Table Column:");
        ComboBox<String> mainColumnCombo = new ComboBox<>();
        mainColumnCombo.setItems(FXCollections.observableArrayList(
            mainColumns.stream().map(ColumnMetadata::name).toList()
        ));
        mainColumnCombo.setPrefWidth(200);
        
        // Subquery table
        Label subqueryTableLabel = new Label("Subquery Table:");
        ComboBox<String> subqueryTableCombo = new ComboBox<>();
        subqueryTableCombo.setItems(tables);
        subqueryTableCombo.setPrefWidth(200);
        
        // Subquery column
        Label subqueryColumnLabel = new Label("Subquery Column:");
        ComboBox<String> subqueryColumnCombo = new ComboBox<>();
        subqueryColumnCombo.setPrefWidth(200);
        
        // Correlated subquery option
        CheckBox correlatedCheckbox = new CheckBox("Correlated Subquery");
        Label correlationColumnLabel = new Label("Correlation Column:");
        ComboBox<String> correlationColumnCombo = new ComboBox<>();
        correlationColumnCombo.setPrefWidth(200);
        correlationColumnCombo.setDisable(true);
        
        correlatedCheckbox.setOnAction(e -> {
            correlationColumnCombo.setDisable(!correlatedCheckbox.isSelected());
        });
        
        // Subquery WHERE conditions
        Label conditionsLabel = new Label("Subquery WHERE Conditions:");
        VBox conditionsContainer = new VBox(5);
        List<TextField> conditionFields = new ArrayList<>();
        
        Button addConditionButton = new Button("Add Condition");
        addConditionButton.setOnAction(e -> {
            TextField condField = new TextField();
            condField.setPromptText("e.g., status = 'active'");
            condField.setPrefWidth(300);
            conditionFields.add(condField);
            conditionsContainer.getChildren().add(condField);
        });
        
        // Load subquery columns when table is selected
        subqueryTableCombo.setOnAction(e -> {
            String subTable = subqueryTableCombo.getValue();
            if (subTable != null && !subTable.isBlank()) {
                runAsyncWithErrors("Loading columns", () -> databaseService.describeTable(subTable), cols -> {
                    List<String> colNames = cols.stream().map(ColumnMetadata::name).toList();
                    subqueryColumnCombo.setItems(FXCollections.observableArrayList(colNames));
                    correlationColumnCombo.setItems(FXCollections.observableArrayList(colNames));
                });
            }
        });
        
        // Toggle visibility based on operator
        operatorCombo.setOnAction(e -> {
            String op = operatorCombo.getValue();
            boolean isExists = "EXISTS".equals(op) || "NOT EXISTS".equals(op);
            mainColumnLabel.setVisible(!isExists);
            mainColumnCombo.setVisible(!isExists);
            mainColumnLabel.setManaged(!isExists);
            mainColumnCombo.setManaged(!isExists);
        });
        
        // Preview area
        TextArea previewArea = new TextArea();
        previewArea.setEditable(false);
        previewArea.setPrefRowCount(4);
        previewArea.setPromptText("SQL preview will appear here...");
        
        Button previewButton = new Button("Preview SQL");
        previewButton.setOnAction(e -> {
            try {
                SubqueryFilter filter = buildSubqueryFilter(
                    mainColumnCombo.getValue(),
                    operatorCombo.getValue(),
                    subqueryTableCombo.getValue(),
                    subqueryColumnCombo.getValue(),
                    conditionFields.stream().map(TextField::getText).filter(s -> s != null && !s.isBlank()).toList(),
                    correlatedCheckbox.isSelected(),
                    correlationColumnCombo.getValue()
                );
                previewArea.setText(filter.toSQL(mainTable));
            } catch (Exception ex) {
                previewArea.setText("Error: " + ex.getMessage());
            }
        });
        
        content.getChildren().addAll(
            operatorLabel, operatorCombo,
            mainColumnLabel, mainColumnCombo,
            subqueryTableLabel, subqueryTableCombo,
            subqueryColumnLabel, subqueryColumnCombo,
            new Separator(),
            correlatedCheckbox,
            correlationColumnLabel, correlationColumnCombo,
            new Separator(),
            conditionsLabel,
            addConditionButton,
            conditionsContainer,
            new Separator(),
            previewButton,
            new Label("SQL Preview:"),
            previewArea
        );
        
        ScrollPane scrollPane = new ScrollPane(content);
        scrollPane.setFitToWidth(true);
        scrollPane.setPrefHeight(550);
        scrollPane.setPrefWidth(450);
        
        dialog.getDialogPane().setContent(scrollPane);
        dialog.getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);
        
        dialog.setResultConverter(buttonType -> {
            if (buttonType == ButtonType.OK) {
                try {
                    return buildSubqueryFilter(
                        mainColumnCombo.getValue(),
                        operatorCombo.getValue(),
                        subqueryTableCombo.getValue(),
                        subqueryColumnCombo.getValue(),
                        conditionFields.stream().map(TextField::getText).filter(s -> s != null && !s.isBlank()).toList(),
                        correlatedCheckbox.isSelected(),
                        correlationColumnCombo.getValue()
                    );
                } catch (Exception ex) {
                    showError(ex.getMessage());
                    return null;
                }
            }
            return null;
        });
        
        dialog.showAndWait().ifPresent(filter -> {
            subqueryFilters.add(filter);
            setStatus("Subquery filter added. Execute query to apply.");
        });
    }

    private SubqueryFilter buildSubqueryFilter(String mainColumn, String operator, String subqueryTable,
                                                String subqueryColumn, List<String> conditions,
                                                boolean isCorrelated, String correlationColumn) {
        boolean isExists = "EXISTS".equals(operator) || "NOT EXISTS".equals(operator);
        
        if (!isExists && (mainColumn == null || mainColumn.isBlank())) {
            throw new IllegalArgumentException("Main table column is required for " + operator);
        }
        if (subqueryTable == null || subqueryTable.isBlank()) {
            throw new IllegalArgumentException("Subquery table is required");
        }
        if (subqueryColumn == null || subqueryColumn.isBlank()) {
            throw new IllegalArgumentException("Subquery column is required");
        }
        
        return new SubqueryFilter(mainColumn, operator, subqueryTable, subqueryColumn, 
                                  conditions, isCorrelated, correlationColumn);
    }

    @FXML
    private void handleClearSubqueryFilters() {
        subqueryFilters.clear();
        setStatus("Subquery filters cleared");
    }

    // ========== CASE EXPRESSION BUILDER ==========

    @FXML
    private void handleShowCaseBuilder() {
        String tableName = dataTableSelector.getValue();
        if (tableName == null || tableName.isBlank()) {
            showError("Please select a table first");
            return;
        }
        
        runAsyncWithErrors("Loading columns", () -> databaseService.describeTable(tableName), columns -> {
            showCaseBuilderDialog(tableName, columns);
        });
    }

    private void showCaseBuilderDialog(String tableName, List<ColumnMetadata> columns) {
        Dialog<ComputedColumn> dialog = new Dialog<>();
        dialog.setTitle("CASE Expression Builder");
        dialog.setHeaderText("Create a computed column with CASE expression");
        
        VBox content = new VBox(15);
        content.setPadding(new Insets(15));
        
        // Alias for the computed column
        Label aliasLabel = new Label("Column Alias:");
        TextField aliasField = new TextField();
        aliasField.setPromptText("e.g., status_text");
        aliasField.setPrefWidth(200);
        
        // WHEN-THEN conditions
        Label whenLabel = new Label("WHEN ... THEN ... conditions:");
        VBox whenContainer = new VBox(10);
        List<HBox> whenRows = new ArrayList<>();
        
        Runnable addWhenRow = () -> {
            HBox row = new HBox(10);
            row.setAlignment(Pos.CENTER_LEFT);
            
            ComboBox<String> columnCombo = new ComboBox<>();
            columnCombo.setItems(FXCollections.observableArrayList(
                columns.stream().map(ColumnMetadata::name).toList()
            ));
            columnCombo.setPromptText("Column");
            columnCombo.setPrefWidth(120);
            
            ComboBox<String> opCombo = new ComboBox<>();
            opCombo.setItems(FXCollections.observableArrayList("=", "!=", ">", "<", ">=", "<=", "IS NULL", "IS NOT NULL"));
            opCombo.setValue("=");
            opCombo.setPrefWidth(80);
            
            TextField valueField = new TextField();
            valueField.setPromptText("Value");
            valueField.setPrefWidth(100);
            
            Label thenLabel = new Label("THEN");
            
            TextField resultField = new TextField();
            resultField.setPromptText("Result");
            resultField.setPrefWidth(100);
            
            Button removeBtn = new Button("X");
            removeBtn.setOnAction(e -> {
                whenRows.remove(row);
                whenContainer.getChildren().remove(row);
            });
            
            row.getChildren().addAll(new Label("WHEN"), columnCombo, opCombo, valueField, thenLabel, resultField, removeBtn);
            whenRows.add(row);
            whenContainer.getChildren().add(row);
        };
        
        Button addWhenButton = new Button("Add WHEN clause");
        addWhenButton.setOnAction(e -> addWhenRow.run());
        
        // Add one WHEN row by default
        addWhenRow.run();
        
        // ELSE clause
        Label elseLabel = new Label("ELSE value (optional):");
        TextField elseField = new TextField();
        elseField.setPromptText("e.g., 'Unknown'");
        elseField.setPrefWidth(200);
        
        // Preview
        TextArea previewArea = new TextArea();
        previewArea.setEditable(false);
        previewArea.setPrefRowCount(4);
        
        Button previewButton = new Button("Preview SQL");
        previewButton.setOnAction(e -> {
            try {
                List<CaseWhen> caseWhens = buildCaseWhens(whenRows);
                ComputedColumn cc = new ComputedColumn(aliasField.getText(), caseWhens, elseField.getText());
                previewArea.setText(cc.toSQL());
            } catch (Exception ex) {
                previewArea.setText("Error: " + ex.getMessage());
            }
        });
        
        content.getChildren().addAll(
            aliasLabel, aliasField,
            new Separator(),
            whenLabel,
            addWhenButton,
            whenContainer,
            new Separator(),
            elseLabel, elseField,
            new Separator(),
            previewButton,
            new Label("SQL Preview:"),
            previewArea
        );
        
        ScrollPane scrollPane = new ScrollPane(content);
        scrollPane.setFitToWidth(true);
        scrollPane.setPrefHeight(500);
        scrollPane.setPrefWidth(600);
        
        dialog.getDialogPane().setContent(scrollPane);
        dialog.getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);
        
        dialog.setResultConverter(buttonType -> {
            if (buttonType == ButtonType.OK) {
                try {
                    List<CaseWhen> caseWhens = buildCaseWhens(whenRows);
                    if (caseWhens.isEmpty()) {
                        throw new IllegalArgumentException("At least one WHEN clause is required");
                    }
                    return new ComputedColumn(aliasField.getText(), caseWhens, elseField.getText());
                } catch (Exception ex) {
                    showError(ex.getMessage());
                    return null;
                }
            }
            return null;
        });
        
        dialog.showAndWait().ifPresent(cc -> {
            computedColumns.add(cc);
            setStatus("CASE expression added. Execute query to see results.");
        });
    }

    @SuppressWarnings("unchecked")
    private List<CaseWhen> buildCaseWhens(List<HBox> whenRows) {
        List<CaseWhen> caseWhens = new ArrayList<>();
        
        for (HBox row : whenRows) {
            ComboBox<String> columnCombo = (ComboBox<String>) row.getChildren().get(1);
            ComboBox<String> opCombo = (ComboBox<String>) row.getChildren().get(2);
            TextField valueField = (TextField) row.getChildren().get(3);
            TextField resultField = (TextField) row.getChildren().get(5);
            
            String column = columnCombo.getValue();
            String op = opCombo.getValue();
            String value = valueField.getText();
            String result = resultField.getText();
            
            if (column == null || column.isBlank()) continue;
            if (result == null || result.isBlank()) continue;
            
            String condition;
            if ("IS NULL".equals(op) || "IS NOT NULL".equals(op)) {
                condition = quoteIdent(column) + " " + op;
            } else {
                String quotedValue = value.matches("-?\\d+(\\.\\d+)?") ? value : "'" + value.replace("'", "''") + "'";
                condition = quoteIdent(column) + " " + op + " " + quotedValue;
            }
            
            String quotedResult = result.matches("-?\\d+(\\.\\d+)?") ? result : "'" + result.replace("'", "''") + "'";
            caseWhens.add(new CaseWhen(condition, quotedResult));
        }
        
        return caseWhens;
    }

    @FXML
    private void handleClearCaseExpressions() {
        computedColumns.clear();
        setStatus("CASE expressions cleared");
    }

    // ========== NULL FUNCTIONS (COALESCE, NULLIF) ==========

    @FXML
    private void handleShowNullFunctions() {
        String tableName = dataTableSelector.getValue();
        if (tableName == null || tableName.isBlank()) {
            showError("Please select a table first");
            return;
        }
        
        runAsyncWithErrors("Loading columns", () -> databaseService.describeTable(tableName), columns -> {
            showNullFunctionsDialog(tableName, columns);
        });
    }

    private void showNullFunctionsDialog(String tableName, List<ColumnMetadata> columns) {
        Dialog<Void> dialog = new Dialog<>();
        dialog. setTitle("NULL Handling Functions");
        dialog. setHeaderText("Apply COALESCE or NULLIF functions");

        VBox content = new VBox(15);
        content. setPadding(new Insets(15));

        // Function selection
        Label functionLabel = new Label("Function:");
        ComboBox<String> functionCombo = new ComboBox<>();
        functionCombo. setItems(FXCollections.observableArrayList("COALESCE", "NULLIF"));
        functionCombo.setValue("COALESCE");
        functionCombo.setPrefWidth(200);

        // Column selection
        Label columnLabel = new Label("Column:");
        ComboBox<String> columnCombo = new ComboBox<>();
        columnCombo. setItems(FXCollections.observableArrayList(
                columns.stream().map(ColumnMetadata::name).toList()
        ));
        columnCombo.setPrefWidth(200);

        // Value input
        Label valueLabel = new Label("Value:");
        TextField valueField = new TextField();
        valueField.setPrefWidth(200);

        // Description updates based on function
        Label descLabel = new Label();
        descLabel. setWrapText(true);
        descLabel.setStyle("-fx-font-size: 11px; -fx-text-fill: gray;");

        functionCombo.setOnAction(e -> {
            String func = functionCombo. getValue();
            if ("COALESCE".equals(func)) {
                valueField.setPromptText("Default value when NULL");
                descLabel.setText("COALESCE returns the first non-NULL value.  Use this to replace NULL with a default.");
            } else {
                valueField. setPromptText("Value to compare");
                descLabel. setText("NULLIF returns NULL if the column value equals the specified value, otherwise returns the column value.");
            }
        });
        functionCombo.fireEvent(new javafx.event.ActionEvent());

        // Result preview
        TextArea resultArea = new TextArea();
        resultArea.setEditable(false);
        resultArea.setPrefRowCount(10);

        Button applyButton = new Button("Apply & Preview");
        applyButton.setOnAction(e -> {
            String function = functionCombo. getValue();
            String column = columnCombo.getValue();
            String value = valueField.getText();

            // Валидация с выводом ошибки в resultArea вместо showError
            if (column == null || column.isBlank()) {
                resultArea.setText("Error: Please select a column");
                return;
            }
            if (value == null || value.isBlank()) {
                resultArea.setText("Error: Please enter a value");
                return;
            }

            String quotedValue = value.matches("-?\\d+(\\.\\d+)? ") ? value : "'" + value. replace("'", "''") + "'";
            String funcExpr = function + "(" + quoteIdent(column) + ", " + quotedValue + ")";

            String sql = "SELECT " + quoteIdent(column) + " AS original, " + funcExpr + " AS result FROM " +
                    quoteIdent(tableName) + " LIMIT 20";

            // Используем модифицированный async вызов с обработкой ошибок в resultArea
            statusLabel.setText("Applying function...");
            CompletableFuture
                    .supplyAsync(() -> {
                        try {
                            return databaseService.executeCustomQuery(sql);
                        } catch (Exception ex) {
                            throw new CompletionException(ex);
                        }
                    }, dbExecutor)
                    .whenComplete((results, throwable) -> Platform.runLater(() -> {
                        if (throwable != null) {
                            // Выводим ошибку в preview вместо showError
                            Throwable cause = throwable instanceof CompletionException && throwable.getCause() != null
                                    ? throwable.getCause() : throwable;
                            String message = cause.getMessage() != null ? cause.getMessage() : cause.toString();
                            resultArea.setText("Error: " + message + "\n\nPlease check that the value type matches the column type.");
                            statusLabel.setText("Function application failed");
                        } else {
                            StringBuilder sb = new StringBuilder();
                            sb. append("Function: ").append(funcExpr).append("\n\n");
                            sb.append(String.format("%-30s | %-30s\n", "Original", "Result"));
                            sb.append("-".repeat(63)). append("\n");

                            for (Map<String, Object> row : results) {
                                Object orig = row.get("original");
                                Object res = row.get("result");
                                sb.append(String. format("%-30s | %-30s\n",
                                        orig != null ? orig.toString() : "NULL",
                                        res != null ? res. toString() : "NULL"));
                            }

                            resultArea.setText(sb.toString());
                            statusLabel.setText("Function applied successfully");
                        }
                    }));
        });

        content.getChildren().addAll(
                functionLabel, functionCombo,
                descLabel,
                columnLabel, columnCombo,
                valueLabel, valueField,
                applyButton,
                new Separator(),
                new Label("Preview (first 20 rows):"),
                resultArea
        );

        ScrollPane scrollPane = new ScrollPane(content);
        scrollPane. setFitToWidth(true);
        scrollPane.setPrefHeight(500);
        scrollPane.setPrefWidth(500);

        dialog.getDialogPane().setContent(scrollPane);
        dialog.getDialogPane().getButtonTypes().add(ButtonType. CLOSE);

        dialog.showAndWait();
    }

    // ========== SIMILAR TO PATTERN MATCHING ==========

    @FXML
    private void handleShowSimilarTo() {
        String tableName = dataTableSelector.getValue();
        if (tableName == null || tableName.isBlank()) {
            showError("Please select a table first");
            return;
        }
        
        runAsyncWithErrors("Loading columns", () -> databaseService.describeTable(tableName), columns -> {
            showSimilarToDialog(tableName, columns);
        });
    }

    private void showSimilarToDialog(String tableName, List<ColumnMetadata> columns) {
        Dialog<Void> dialog = new Dialog<>();
        dialog.setTitle("SIMILAR TO Pattern Matching");
        dialog.setHeaderText("Search strings using SQL regex patterns (SIMILAR TO)");
        
        VBox content = new VBox(15);
        content.setPadding(new Insets(15));
        
        // Column selection
        Label columnLabel = new Label("Column:");
        ComboBox<String> columnCombo = new ComboBox<>();
        columnCombo.setItems(FXCollections.observableArrayList(
            columns.stream().map(ColumnMetadata::name).toList()
        ));
        columnCombo.setPrefWidth(250);
        
        // Pattern input
        Label patternLabel = new Label("Pattern:");
        TextField patternField = new TextField();
        patternField.setPromptText("e.g., %(abc|def)%");
        patternField.setPrefWidth(300);
        
        // Negation checkbox
        CheckBox notCheckbox = new CheckBox("NOT SIMILAR TO (negate)");
        
        // Pattern help
        TextArea helpArea = new TextArea();
        helpArea.setEditable(false);
        helpArea.setPrefRowCount(8);
        helpArea.setText("""
            SIMILAR TO Pattern Syntax:
            
            %           - matches any sequence of characters
            _           - matches any single character
            |           - alternation (OR)
            (...)       - grouping
            [abc]       - character class
            [a-z]       - character range
            *           - repeat 0 or more times
            +           - repeat 1 or more times
            ?           - repeat 0 or 1 time
            {n}         - repeat exactly n times
            
            Example: '%(cat|dog)%' matches strings containing 'cat' or 'dog'
            """);
        helpArea.setStyle("-fx-font-size: 11px;");
        
        // Results
        TableView<Map<String, Object>> resultsTable = new TableView<>();
        resultsTable.setPrefHeight(200);
        
        Button searchButton = new Button("Search");
        searchButton.setOnAction(e -> {
            String column = columnCombo.getValue();
            String pattern = patternField.getText();
            
            if (column == null || column.isBlank()) {
                showError("Please select a column");
                return;
            }
            if (pattern == null || pattern.isBlank()) {
                showError("Please enter a pattern");
                return;
            }
            
            String operator = notCheckbox.isSelected() ? "NOT SIMILAR TO" : "SIMILAR TO";
            String sql = "SELECT * FROM " + quoteIdent(tableName) + 
                         " WHERE " + quoteIdent(column) + " " + operator + " '" + pattern.replace("'", "''") + "'";
            
            runAsyncWithErrors("Searching", 
                () -> databaseService.executeCustomQuery(sql),
                results -> {
                    resultsTable.getColumns().clear();
                    resultsTable.getItems().clear();
                    
                    if (!results.isEmpty()) {
                        Map<String, Object> firstRow = results.get(0);
                        for (String key : firstRow.keySet()) {
                            TableColumn<Map<String, Object>, Object> col = new TableColumn<>(key);
                            col.setCellValueFactory(param -> new ReadOnlyObjectWrapper<>(param.getValue().get(key)));
                            col.setPrefWidth(120);
                            resultsTable.getColumns().add(col);
                        }
                        resultsTable.getItems().addAll(results);
                    }
                    
                    setStatus("Found " + results.size() + " matching row(s)");
                });
        });
        
        content.getChildren().addAll(
            columnLabel, columnCombo,
            patternLabel, patternField,
            notCheckbox,
            searchButton,
            new Separator(),
            new Label("Pattern Syntax Help:"),
            helpArea,
            new Separator(),
            new Label("Results:"),
            resultsTable
        );
        
        ScrollPane scrollPane = new ScrollPane(content);
        scrollPane.setFitToWidth(true);
        scrollPane.setPrefHeight(650);
        scrollPane.setPrefWidth(600);
        
        dialog.getDialogPane().setContent(scrollPane);
        dialog.getDialogPane().getButtonTypes().add(ButtonType.CLOSE);
        
        dialog.showAndWait();
    }

    // ========== USER-DEFINED TYPES (COMPOSITE TYPES) ==========

    @FXML
    private void handleShowCompositeTypes() {
        showCompositeTypesDialog();
    }

    private void showCompositeTypesDialog() {
        Dialog<Void> dialog = new Dialog<>();
        dialog.setTitle("User-Defined Types");
        dialog.setHeaderText("Create and manage composite types");
        
        TabPane tabPane = new TabPane();
        
        // Tab 1: Create Composite Type
        Tab createTab = new Tab("Create Composite Type");
        createTab.setClosable(false);
        
        VBox createContent = new VBox(15);
        createContent.setPadding(new Insets(15));
        
        Label typeNameLabel = new Label("Type Name:");
        TextField typeNameField = new TextField();
        typeNameField.setPromptText("e.g., address_type");
        typeNameField.setPrefWidth(250);
        
        Label fieldsLabel = new Label("Fields (name and type):");
        VBox fieldsContainer = new VBox(5);
        List<HBox> fieldRows = new ArrayList<>();
        
        Runnable addFieldRow = () -> {
            HBox row = new HBox(10);
            row.setAlignment(Pos.CENTER_LEFT);
            
            TextField nameField = new TextField();
            nameField.setPromptText("Field name");
            nameField.setPrefWidth(150);
            
            ComboBox<String> typeCombo = new ComboBox<>();
            typeCombo.setItems(FXCollections.observableArrayList(availableTypes));
            typeCombo.setEditable(true);
            typeCombo.setPromptText("Type");
            typeCombo.setPrefWidth(150);
            
            Button removeBtn = new Button("X");
            removeBtn.setOnAction(e -> {
                fieldRows.remove(row);
                fieldsContainer.getChildren().remove(row);
            });
            
            row.getChildren().addAll(nameField, typeCombo, removeBtn);
            fieldRows.add(row);
            fieldsContainer.getChildren().add(row);
        };
        
        Button addFieldButton = new Button("Add Field");
        addFieldButton.setOnAction(e -> addFieldRow.run());
        
        // Add two default fields
        addFieldRow.run();
        addFieldRow.run();
        
        Button createTypeButton = new Button("Create Composite Type");
        createTypeButton.setOnAction(e -> {
            String typeName = typeNameField.getText();
            if (typeName == null || typeName.isBlank()) {
                showError("Please enter a type name");
                return;
            }
            
            List<String> fieldDefs = new ArrayList<>();
            for (HBox row : fieldRows) {
                TextField nameField = (TextField) row.getChildren().get(0);
                @SuppressWarnings("unchecked")
                ComboBox<String> typeCombo = (ComboBox<String>) row.getChildren().get(1);
                
                String fieldName = nameField.getText();
                String fieldType = typeCombo.getValue();
                
                if (fieldName != null && !fieldName.isBlank() && fieldType != null && !fieldType.isBlank()) {
                    fieldDefs.add(quoteIdent(fieldName) + " " + fieldType);
                }
            }
            
            if (fieldDefs.isEmpty()) {
                showError("At least one field is required");
                return;
            }
            
            runAsyncVoid("Creating composite type",
                () -> databaseService.createCompositeType(typeName, fieldDefs),
                () -> {
                    setStatus("Composite type '" + typeName + "' created successfully");
                    typeNameField.clear();
                    handleRefreshEnums(); // Refresh types list
                });
        });
        
        createContent.getChildren().addAll(
            typeNameLabel, typeNameField,
            new Separator(),
            fieldsLabel,
            addFieldButton,
            fieldsContainer,
            new Separator(),
            createTypeButton
        );
        
        createTab.setContent(new ScrollPane(createContent));
        
        // Tab 2: View/Manage Existing Types
        Tab viewTab = new Tab("View Types");
        viewTab.setClosable(false);
        
        VBox viewContent = new VBox(15);
        viewContent.setPadding(new Insets(15));
        
        Label enumsLabel = new Label("ENUM Types:");
        ListView<String> enumsList = new ListView<>();
        enumsList.setItems(enums);
        enumsList.setPrefHeight(150);
        
        Button viewEnumButton = new Button("View ENUM Values");
        viewEnumButton.setOnAction(e -> {
            String selected = enumsList.getSelectionModel().getSelectedItem();
            if (selected != null) {
                showEnumValues(selected);
            }
        });
        
        Button dropEnumButton = new Button("Drop ENUM Type");
        dropEnumButton.setOnAction(e -> {
            String selected = enumsList.getSelectionModel().getSelectedItem();
            if (selected != null) {
                Alert confirm = new Alert(Alert.AlertType.CONFIRMATION);
                confirm.setTitle("Confirm Drop");
                confirm.setHeaderText("Drop ENUM type '" + selected + "'?");
                confirm.setContentText("This will fail if the type is in use by any table.");
                
                confirm.showAndWait().ifPresent(response -> {
                    if (response == ButtonType.OK) {
                        runAsyncVoid("Dropping ENUM type",
                            () -> databaseService.dropType(selected),
                            () -> {
                                setStatus("ENUM type '" + selected + "' dropped");
                                handleRefreshEnums();
                            });
                    }
                });
            }
        });
        
        Label compositeLabel = new Label("Composite Types:");
        ListView<String> compositeList = new ListView<>();
        compositeList.setPrefHeight(150);
        
        Button refreshTypesButton = new Button("Refresh Types");
        refreshTypesButton.setOnAction(e -> {
            runAsyncWithErrors("Loading composite types",
                () -> databaseService.listCompositeTypes(),
                types -> {
                    compositeList.setItems(FXCollections.observableArrayList(types));
                });
        });
        
        Button viewCompositeButton = new Button("View Composite Structure");
        viewCompositeButton.setOnAction(e -> {
            String selected = compositeList.getSelectionModel().getSelectedItem();
            if (selected != null) {
                runAsyncWithErrors("Loading type structure",
                    () -> databaseService.getCompositeTypeFields(selected),
                    fields -> {
                        Alert info = new Alert(Alert.AlertType.INFORMATION);
                        info.setTitle("Composite Type Structure");
                        info.setHeaderText("Fields of type: " + selected);
                        info.setContentText(String.join("\n", fields));
                        info.showAndWait();
                    });
            }
        });
        
        Button dropCompositeButton = new Button("Drop Composite Type");
        dropCompositeButton.setOnAction(e -> {
            String selected = compositeList.getSelectionModel().getSelectedItem();
            if (selected != null) {
                Alert confirm = new Alert(Alert.AlertType.CONFIRMATION);
                confirm.setTitle("Confirm Drop");
                confirm.setHeaderText("Drop composite type '" + selected + "'?");
                
                confirm.showAndWait().ifPresent(response -> {
                    if (response == ButtonType.OK) {
                        runAsyncVoid("Dropping composite type",
                            () -> databaseService.dropType(selected),
                            () -> {
                                setStatus("Composite type '" + selected + "' dropped");
                                refreshTypesButton.fire();
                            });
                    }
                });
            }
        });
        
        // Load composite types on open
        Platform.runLater(() -> refreshTypesButton.fire());
        
        viewContent.getChildren().addAll(
            enumsLabel, enumsList,
            new HBox(10, viewEnumButton, dropEnumButton),
            new Separator(),
            compositeLabel, compositeList,
            new HBox(10, refreshTypesButton, viewCompositeButton, dropCompositeButton)
        );
        
        viewTab.setContent(new ScrollPane(viewContent));
        
        // Tab 3: Add ENUM value
        Tab addEnumTab = new Tab("Modify ENUM");
        addEnumTab.setClosable(false);
        
        VBox addEnumContent = new VBox(15);
        addEnumContent.setPadding(new Insets(15));
        
        Label selectEnumLabel = new Label("Select ENUM Type:");
        ComboBox<String> enumCombo = new ComboBox<>();
        enumCombo.setItems(enums);
        enumCombo.setPrefWidth(200);
        
        Label newValueLabel = new Label("New Value:");
        TextField newValueField = new TextField();
        newValueField.setPromptText("e.g., new_status");
        newValueField.setPrefWidth(200);
        
        Label positionLabel = new Label("Position:");
        ComboBox<String> positionCombo = new ComboBox<>();
        positionCombo.setItems(FXCollections.observableArrayList("At end", "Before existing value", "After existing value"));
        positionCombo.setValue("At end");
        positionCombo.setPrefWidth(200);
        
        Label refValueLabel = new Label("Reference Value:");
        ComboBox<String> refValueCombo = new ComboBox<>();
        refValueCombo.setPrefWidth(200);
        refValueCombo.setDisable(true);
        
        positionCombo.setOnAction(e -> {
            String pos = positionCombo.getValue();
            refValueCombo.setDisable("At end".equals(pos));
            if (!"At end".equals(pos)) {
                String selectedEnum = enumCombo.getValue();
                if (selectedEnum != null) {
                    runAsyncWithErrors("Loading enum values",
                        () -> databaseService.getEnumValues(selectedEnum),
                        values -> refValueCombo.setItems(FXCollections.observableArrayList(values)));
                }
            }
        });
        
        enumCombo.setOnAction(e -> {
            String pos = positionCombo.getValue();
            if (!"At end".equals(pos)) {
                String selectedEnum = enumCombo.getValue();
                if (selectedEnum != null) {
                    runAsyncWithErrors("Loading enum values",
                        () -> databaseService.getEnumValues(selectedEnum),
                        values -> refValueCombo.setItems(FXCollections.observableArrayList(values)));
                }
            }
        });
        
        Button addValueButton = new Button("Add Value to ENUM");
        addValueButton.setOnAction(e -> {
            String enumName = enumCombo.getValue();
            String newValue = newValueField.getText();
            String position = positionCombo.getValue();
            String refValue = refValueCombo.getValue();
            
            if (enumName == null || enumName.isBlank()) {
                showError("Please select an ENUM type");
                return;
            }
            if (newValue == null || newValue.isBlank()) {
                showError("Please enter a new value");
                return;
            }
            
            runAsyncVoid("Adding value to ENUM",
                () -> databaseService.addEnumValue(enumName, newValue, position, refValue),
                () -> {
                    setStatus("Value '" + newValue + "' added to ENUM '" + enumName + "'");
                    newValueField.clear();
                });
        });
        
        addEnumContent.getChildren().addAll(
            selectEnumLabel, enumCombo,
            newValueLabel, newValueField,
            positionLabel, positionCombo,
            refValueLabel, refValueCombo,
            new Separator(),
            addValueButton
        );
        
        addEnumTab.setContent(new ScrollPane(addEnumContent));
        
        tabPane.getTabs().addAll(createTab, viewTab, addEnumTab);
        
        dialog.getDialogPane().setContent(tabPane);
        dialog.getDialogPane().setPrefWidth(500);
        dialog.getDialogPane().setPrefHeight(500);
        dialog.getDialogPane().getButtonTypes().add(ButtonType.CLOSE);
        
        dialog.showAndWait();
    }

    // ========== JOIN WIZARD HANDLERS ==========

    @FXML
    private void handleJoinLeftTableSelected() {
        // Refresh join combos when left table changes
    }

    @FXML
    private void handleAddJoin() {
        String leftTable = joinLeftTableCombo != null ? joinLeftTableCombo.getValue() : null;
        if (leftTable == null || leftTable.isBlank()) {
            showError("Please select a primary table first");
            return;
        }
        
        // Create dialog for join configuration
        Dialog<JoinRow> dialog = new Dialog<>();
        dialog.setTitle("Add JOIN");
        dialog.setHeaderText("Configure JOIN clause");
        
        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);
        grid.setPadding(new Insets(20));
        
        ComboBox<String> joinTypeCombo = new ComboBox<>();
        joinTypeCombo.setItems(FXCollections.observableArrayList(
            "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"
        ));
        joinTypeCombo.setValue("INNER JOIN");
        joinTypeCombo.setPrefWidth(150);
        
        ComboBox<String> rightTableCombo = new ComboBox<>();
        rightTableCombo.setItems(tables);
        rightTableCombo.setPrefWidth(200);
        
        TextField rightAliasField = new TextField();
        rightAliasField.setPromptText("Optional");
        rightAliasField.setPrefWidth(100);
        
        ComboBox<String> leftColumnCombo = new ComboBox<>();
        leftColumnCombo.setPrefWidth(150);
        
        ComboBox<String> rightColumnCombo = new ComboBox<>();
        rightColumnCombo.setPrefWidth(150);
        
        // Load columns for left table
        runAsyncWithErrors("Loading columns", () -> databaseService.describeTable(leftTable), columns -> {
            List<String> columnNames = columns.stream().map(ColumnMetadata::name).toList();
            leftColumnCombo.setItems(FXCollections.observableArrayList(columnNames));
        });
        
        // Load columns when right table is selected
        rightTableCombo.setOnAction(e -> {
            String rightTable = rightTableCombo.getValue();
            if (rightTable != null && !rightTable.isBlank()) {
                runAsyncWithErrors("Loading columns", () -> databaseService.describeTable(rightTable), columns -> {
                    List<String> columnNames = columns.stream().map(ColumnMetadata::name).toList();
                    rightColumnCombo.setItems(FXCollections.observableArrayList(columnNames));
                });
            }
        });
        
        grid.add(new Label("JOIN Type:"), 0, 0);
        grid.add(joinTypeCombo, 1, 0);
        
        grid.add(new Label("Right Table:"), 0, 1);
        grid.add(rightTableCombo, 1, 1);
        
        grid.add(new Label("Right Table Alias:"), 0, 2);
        grid.add(rightAliasField, 1, 2);
        
        grid.add(new Label("Left Column:"), 0, 3);
        grid.add(leftColumnCombo, 1, 3);
        
        grid.add(new Label("Right Column:"), 0, 4);
        grid.add(rightColumnCombo, 1, 4);
        
        dialog.getDialogPane().setContent(grid);
        dialog.getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);
        
        dialog.setResultConverter(buttonType -> {
            if (buttonType == ButtonType.OK) {
                String joinType = joinTypeCombo.getValue();
                String rightTable = rightTableCombo.getValue();
                String rightAlias = rightAliasField.getText();
                String leftCol = leftColumnCombo.getValue();
                String rightCol = rightColumnCombo.getValue();
                
                if (rightTable == null || leftCol == null || rightCol == null) {
                    showError("Please fill all required fields");
                    return null;
                }
                
                VBox container = new VBox(5);
                JoinRow row = new JoinRow(container, joinType, rightTable, rightAlias, leftCol, rightCol);
                return row;
            }
            return null;
        });
        
        dialog.showAndWait().ifPresent(row -> {
            joinRows.add(row);
            
            // Create visual representation
            HBox joinDisplay = new HBox(10);
            joinDisplay.setAlignment(Pos.CENTER_LEFT);
            
            String displayText = row.joinType + " " + row.rightTable +
                (row.rightAlias != null && !row.rightAlias.isBlank() ? " AS " + row.rightAlias : "") +
                " ON " + row.leftColumn + " = " + row.rightColumn;
            
            Label joinLabel = new Label(displayText);
            Button removeButton = new Button("Remove");
            
            removeButton.setOnAction(e -> {
                joinRows.remove(row);
                if (joinRowsContainer != null) {
                    joinRowsContainer.getChildren().remove(row.container);
                }
            });
            
            joinDisplay.getChildren().addAll(joinLabel, removeButton);
            row.container.getChildren().add(joinDisplay);
            
            if (joinRowsContainer != null) {
                joinRowsContainer.getChildren().add(row.container);
            }
        });
    }

    @FXML
    private void handleClearJoins() {
        joinRows.clear();
        if (joinRowsContainer != null) {
            joinRowsContainer.getChildren().clear();
        }
    }

    @FXML
    private void handleChooseJoinColumns() {
        String leftTable = joinLeftTableCombo != null ? joinLeftTableCombo.getValue() : null;
        if (leftTable == null) {
            showError("Please select a primary table first");
            return;
        }
        
        // Collect all tables involved in the join
        List<String> allTables = new ArrayList<>();
        allTables.add(leftTable);
        for (JoinRow row : joinRows) {
            allTables.add(row.rightTable);
        }
        
        // Get all columns from all tables
        List<CompletableFuture<List<ColumnMetadata>>> futures = allTables.stream()
            .map(table -> CompletableFuture.supplyAsync(() -> databaseService.describeTable(table), dbExecutor))
            .toList();
        
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> {
                List<String> allColumns = new ArrayList<>();
                for (int i = 0; i < futures.size(); i++) {
                    try {
                        String table = allTables.get(i);
                        List<ColumnMetadata> columns = futures.get(i).get();
                        for (ColumnMetadata col : columns) {
                            allColumns.add(table + "." + col.name());
                        }
                    } catch (Exception ignored) {
                    }
                }
                return allColumns;
            })
            .whenComplete((allColumns, throwable) -> Platform.runLater(() -> {
                if (throwable != null) {
                    showError("Error loading columns: " + throwable.getMessage());
                    return;
                }
                showJoinColumnSelectionDialog(allColumns);
            }));
    }

    private void showJoinColumnSelectionDialog(List<String> allColumns) {
        Dialog<List<String>> dialog = new Dialog<>();
        dialog.setTitle("Select Columns");
        dialog.setHeaderText("Choose columns to display in result");
        
        VBox content = new VBox(10);
        List<CheckBox> checkBoxes = new ArrayList<>();
        
        for (String col : allColumns) {
            CheckBox cb = new CheckBox(col);
            cb.setSelected(joinSelectedColumns.isEmpty() || joinSelectedColumns.contains(col));
            checkBoxes.add(cb);
            content.getChildren().add(cb);
        }
        
        ScrollPane scrollPane = new ScrollPane(content);
        scrollPane.setFitToWidth(true);
        scrollPane.setPrefHeight(400);
        
        dialog.getDialogPane().setContent(scrollPane);
        dialog.getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);
        
        dialog.setResultConverter(buttonType -> {
            if (buttonType == ButtonType.OK) {
                return checkBoxes.stream()
                    .filter(CheckBox::isSelected)
                    .map(CheckBox::getText)
                    .toList();
            }
            return null;
        });
        
        dialog.showAndWait().ifPresent(selected -> {
            joinSelectedColumns = new ArrayList<>(selected);
            if (joinSelectedColumnsLabel != null) {
                joinSelectedColumnsLabel.setText(selected.isEmpty() ? 
                    "All columns will be displayed" : 
                    "(" + selected.size() + " columns selected)");
            }
        });
    }

    @FXML
    private void handleShowJoinSQL() {
        try {
            String sql = buildJoinSQL();
            
            Alert alert = new Alert(Alert.AlertType.INFORMATION);
            alert.setTitle("Generated SQL");
            alert.setHeaderText("JOIN Query");
            
            TextArea textArea = new TextArea(sql);
            textArea.setEditable(false);
            textArea.setWrapText(true);
            textArea.setPrefRowCount(15);
            
            alert.getDialogPane().setContent(textArea);
            alert.showAndWait();
        } catch (Exception ex) {
            showError("Error building SQL: " + ex.getMessage());
        }
    }

    @FXML
    private void handleExecuteJoin() {
        try {
            String sql = buildJoinSQL();
            
            runAsyncWithErrors("Executing JOIN", 
                () -> databaseService.executeCustomQuery(sql),
                results -> {
                    populateJoinTable(results);
                    setStatus("JOIN returned " + results.size() + " row(s)");
                });
        } catch (Exception ex) {
            showError("Error executing JOIN: " + ex.getMessage());
        }
    }

    private String buildJoinSQL() {
        String leftTable = joinLeftTableCombo != null ? joinLeftTableCombo.getValue() : null;
        if (leftTable == null || leftTable.isBlank()) {
            throw new IllegalArgumentException("Primary table not selected");
        }
        
        String leftAlias = (joinLeftAliasField != null && joinLeftAliasField.getText() != null && !joinLeftAliasField.getText().isBlank()) 
            ? joinLeftAliasField.getText() : leftTable;
        
        StringBuilder sql = new StringBuilder("SELECT ");
        
        if (joinSelectedColumns.isEmpty()) {
            sql.append("*");
        } else {
            sql.append(String.join(", ", joinSelectedColumns));
        }
        
        sql.append(" FROM ").append(quoteIdent(leftTable));
        if (!leftAlias.equals(leftTable)) {
            sql.append(" AS ").append(quoteIdent(leftAlias));
        }
        
        for (JoinRow row : joinRows) {
            sql.append(" ").append(row.toSQL(leftAlias));
        }
        
        String where = joinWhereField != null ? joinWhereField.getText() : null;
        if (where != null && !where.isBlank()) {
            sql.append(" WHERE ").append(where);
        }
        
        String orderBy = joinOrderByField != null ? joinOrderByField.getText() : null;
        if (orderBy != null && !orderBy.isBlank()) {
            sql.append(" ORDER BY ").append(orderBy);
        }
        
        String limit = joinLimitField != null ? joinLimitField.getText() : null;
        if (limit != null && !limit.isBlank()) {
            sql.append(" LIMIT ").append(limit);
        }
        
        return sql.toString();
    }

    private void populateJoinTable(List<Map<String, Object>> data) {
        if (joinResultsTableView == null) {
            return;
        }
        
        joinResultsTableView.getColumns().clear();
        joinResultsTableView.getItems().clear();
        
        if (data.isEmpty()) {
            return;
        }
        
        Map<String, Object> firstRow = data.get(0);
        for (String columnName : firstRow.keySet()) {
            TableColumn<Map<String, Object>, Object> column = new TableColumn<>(columnName);
            column.setCellValueFactory(param -> {
                Object value = param.getValue().get(columnName);
                return new ReadOnlyObjectWrapper<>(value);
            });
            joinResultsTableView.getColumns().add(column);
        }
        
        joinResultsTableView.getItems().addAll(data);
    }

    // ========== ALTER TABLE HANDLERS ==========

    @FXML
    private void handleAlterTableSelected() {
        String tableName = alterTableSelector.getValue();
        if (tableName == null || tableName.isBlank()) {
            clearAlterTableColumnSelectors();
            return;
        }
        
        runAsyncWithErrors("Loading columns", () -> databaseService.describeTable(tableName), metadata -> {
            List<String> columnNames = metadata.stream().map(ColumnMetadata::name).toList();
            if (dropColumnSelector != null) {
                dropColumnSelector.setItems(FXCollections.observableArrayList(columnNames));
            }
            if (renameColumnSelector != null) {
                renameColumnSelector.setItems(FXCollections.observableArrayList(columnNames));
            }
            if (alterColumnTypeSelector != null) {
                alterColumnTypeSelector.setItems(FXCollections.observableArrayList(columnNames));
            }
            if (notNullColumnSelector != null) {
                notNullColumnSelector.setItems(FXCollections.observableArrayList(columnNames));
            }
        });
    }

    @FXML
    private void handleRefreshAlterTables() {
        handleRefreshTables();
    }

    @FXML
    private void handleRenameTable() {
        String oldName = alterTableSelector.getValue();
        String newName = newTableNameField != null ? newTableNameField.getText() : null;
        
        if (oldName == null || oldName.isBlank()) {
            showError("Please select a table");
            return;
        }
        if (newName == null || newName.isBlank()) {
            showError("Please enter new table name");
            return;
        }
        
        Alert confirm = new Alert(Alert.AlertType.CONFIRMATION);
        confirm.setTitle("Confirm Rename");
        confirm.setHeaderText("Rename table '" + oldName + "' to '" + newName + "'?");
        confirm.setContentText("This operation cannot be undone.");
        
        confirm.showAndWait().ifPresent(response -> {
            if (response == ButtonType.OK) {
                runAsyncVoid("Renaming table", 
                    () -> databaseService.renameTable(oldName, newName),
                    () -> {
                        setStatus("Table renamed successfully");
                        if (newTableNameField != null) {
                            newTableNameField.clear();
                        }
                        handleRefreshTables();
                        alterTableSelector.getSelectionModel().select(newName);
                    });
            }
        });
    }

    @FXML
    private void handleAddColumnToTable() {
        String tableName = alterTableSelector.getValue();
        String columnName = addColumnNameField != null ? addColumnNameField.getText() : null;
        String columnType = addColumnTypeCombo != null ? addColumnTypeCombo.getValue() : null;
        String constraints = addColumnConstraintsField != null ? addColumnConstraintsField.getText() : "";
        
        if (tableName == null || tableName.isBlank()) {
            showError("Please select a table");
            return;
        }
        if (columnName == null || columnName.isBlank()) {
            showError("Please enter column name");
            return;
        }
        if (columnType == null || columnType.isBlank()) {
            showError("Please select column type");
            return;
        }
        
        runAsyncVoid("Adding column",
            () -> databaseService.addColumn(tableName, columnName, columnType, constraints),
            () -> {
                setStatus("Column '" + columnName + "' added to table '" + tableName + "'");
                if (addColumnNameField != null) addColumnNameField.clear();
                if (addColumnTypeCombo != null) addColumnTypeCombo.getSelectionModel().clearSelection();
                if (addColumnConstraintsField != null) addColumnConstraintsField.clear();
                handleAlterTableSelected(); // Refresh column lists
            });
    }

    @FXML
    private void handleDropColumn() {
        String tableName = alterTableSelector.getValue();
        String columnName = dropColumnSelector != null ? dropColumnSelector.getValue() : null;
        
        if (tableName == null || tableName.isBlank()) {
            showError("Please select a table");
            return;
        }
        if (columnName == null || columnName.isBlank()) {
            showError("Please select a column to drop");
            return;
        }
        
        Alert confirm = new Alert(Alert.AlertType.CONFIRMATION);
        confirm.setTitle("Confirm Drop Column");
        confirm.setHeaderText("Drop column '" + columnName + "' from table '" + tableName + "'?");
        confirm.setContentText("This operation cannot be undone and all data in this column will be lost.");
        
        confirm.showAndWait().ifPresent(response -> {
            if (response == ButtonType.OK) {
                runAsyncVoid("Dropping column",
                    () -> databaseService.dropColumn(tableName, columnName),
                    () -> {
                        setStatus("Column '" + columnName + "' dropped from table '" + tableName + "'");
                        handleAlterTableSelected(); // Refresh column lists
                    });
            }
        });
    }

    @FXML
    private void handleRenameColumn() {
        String tableName = alterTableSelector.getValue();
        String oldName = renameColumnSelector != null ? renameColumnSelector.getValue() : null;
        String newName = newColumnNameField != null ? newColumnNameField.getText() : null;
        
        if (tableName == null || tableName.isBlank()) {
            showError("Please select a table");
            return;
        }
        if (oldName == null || oldName.isBlank()) {
            showError("Please select a column to rename");
            return;
        }
        if (newName == null || newName.isBlank()) {
            showError("Please enter new column name");
            return;
        }
        
        runAsyncVoid("Renaming column",
            () -> databaseService.renameColumn(tableName, oldName, newName),
            () -> {
                setStatus("Column renamed from '" + oldName + "' to '" + newName + "'");
                if (newColumnNameField != null) {
                    newColumnNameField.clear();
                }
                handleAlterTableSelected(); // Refresh column lists
            });
    }

    @FXML
    private void handleAlterColumnType() {
        String tableName = alterTableSelector.getValue();
        String columnName = alterColumnTypeSelector != null ? alterColumnTypeSelector.getValue() : null;
        String newType = newColumnTypeCombo != null ? newColumnTypeCombo.getValue() : null;
        boolean useCast = usingCastCheckbox != null && usingCastCheckbox.isSelected();
        
        if (tableName == null || tableName.isBlank()) {
            showError("Please select a table");
            return;
        }
        if (columnName == null || columnName.isBlank()) {
            showError("Please select a column");
            return;
        }
        if (newType == null || newType.isBlank()) {
            showError("Please select new type");
            return;
        }
        
        Alert confirm = new Alert(Alert.AlertType.CONFIRMATION);
        confirm.setTitle("Confirm Type Change");
        confirm.setHeaderText("Change type of column '" + columnName + "' to " + newType + "?");
        confirm.setContentText("This may fail if existing data is incompatible. " +
                              (useCast ? "USING cast will be applied." : ""));
        
        confirm.showAndWait().ifPresent(response -> {
            if (response == ButtonType.OK) {
                runAsyncVoid("Changing column type",
                    () -> databaseService.alterColumnType(tableName, columnName, newType, useCast),
                    () -> {
                        setStatus("Column type changed successfully");
                        if (newColumnTypeCombo != null) {
                            newColumnTypeCombo.getSelectionModel().clearSelection();
                        }
                    });
            }
        });
    }

    @FXML
    private void handleAddConstraint() {
        String tableName = alterTableSelector.getValue();
        String constraintName = constraintNameField != null ? constraintNameField.getText() : "";
        String constraintDef = constraintDefinitionField != null ? constraintDefinitionField.getText() : null;
        
        if (tableName == null || tableName.isBlank()) {
            showError("Please select a table");
            return;
        }
        if (constraintDef == null || constraintDef.isBlank()) {
            showError("Please enter constraint definition");
            return;
        }
        
        runAsyncVoid("Adding constraint",
            () -> databaseService.addConstraint(tableName, constraintName, constraintDef),
            () -> {
                setStatus("Constraint added successfully");
                if (constraintNameField != null) constraintNameField.clear();
                if (constraintDefinitionField != null) constraintDefinitionField.clear();
            });
    }

    @FXML
    private void handleDropConstraint() {
        String tableName = alterTableSelector.getValue();
        String constraintName = dropConstraintNameField != null ? dropConstraintNameField.getText() : null;
        
        if (tableName == null || tableName.isBlank()) {
            showError("Please select a table");
            return;
        }
        if (constraintName == null || constraintName.isBlank()) {
            showError("Please enter constraint name");
            return;
        }
        
        Alert confirm = new Alert(Alert.AlertType.CONFIRMATION);
        confirm.setTitle("Confirm Drop Constraint");
        confirm.setHeaderText("Drop constraint '" + constraintName + "' from table '" + tableName + "'?");
        
        confirm.showAndWait().ifPresent(response -> {
            if (response == ButtonType.OK) {
                runAsyncVoid("Dropping constraint",
                    () -> databaseService.dropConstraint(tableName, constraintName),
                    () -> {
                        setStatus("Constraint dropped successfully");
                        if (dropConstraintNameField != null) {
                            dropConstraintNameField.clear();
                        }
                    });
            }
        });
    }

    @FXML
    private void handleSetNotNull() {
        String tableName = alterTableSelector.getValue();
        String columnName = notNullColumnSelector != null ? notNullColumnSelector.getValue() : null;
        
        if (tableName == null || tableName.isBlank()) {
            showError("Please select a table");
            return;
        }
        if (columnName == null || columnName.isBlank()) {
            showError("Please select a column");
            return;
        }
        
        runAsyncVoid("Setting NOT NULL",
            () -> databaseService.setNotNull(tableName, columnName),
            () -> setStatus("NOT NULL constraint set on column '" + columnName + "'"));
    }

    @FXML
    private void handleDropNotNull() {
        String tableName = alterTableSelector.getValue();
        String columnName = notNullColumnSelector != null ? notNullColumnSelector.getValue() : null;
        
        if (tableName == null || tableName.isBlank()) {
            showError("Please select a table");
            return;
        }
        if (columnName == null || columnName.isBlank()) {
            showError("Please select a column");
            return;
        }
        
        runAsyncVoid("Dropping NOT NULL",
            () -> databaseService.dropNotNull(tableName, columnName),
            () -> setStatus("NOT NULL constraint dropped from column '" + columnName + "'"));
    }

    private void clearAlterTableColumnSelectors() {
        if (dropColumnSelector != null) {
            dropColumnSelector.setItems(FXCollections.observableArrayList());
        }
        if (renameColumnSelector != null) {
            renameColumnSelector.setItems(FXCollections.observableArrayList());
        }
        if (alterColumnTypeSelector != null) {
            alterColumnTypeSelector.setItems(FXCollections.observableArrayList());
        }
        if (notNullColumnSelector != null) {
            notNullColumnSelector.setItems(FXCollections.observableArrayList());
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

    private static class ColumnRow {
        private final HBox container;
        private final TextField nameField;
        private final ComboBox<String> typeCombo;
        private final TextField constraintsField;
        private final Button removeButton;

        private ColumnRow(HBox container,
                          TextField nameField,
                          ComboBox<String> typeCombo,
                          TextField constraintsField,
                          Button removeButton) {
            this.container = container;
            this.nameField = nameField;
            this.typeCombo = typeCombo;
            this.constraintsField = constraintsField;
            this.removeButton = removeButton;
        }

        private void setAvailableTypes(List<String> types) {
            String previous = typeCombo.getValue();
            typeCombo.setItems(FXCollections.observableArrayList(types));
            if (previous != null && types.contains(previous)) {
                typeCombo.setValue(previous);
            } else {
                typeCombo.getSelectionModel().clearSelection();
            }
        }

        private void clear() {
            nameField.clear();
            typeCombo.getSelectionModel().clearSelection();
            constraintsField.clear();
        }
    }

    @SuppressWarnings("unused")
    private static class SortRow {
        private final HBox container;
        private final Label columnLabel;
        private final Label directionLabel;
        private final Button removeButton;
        private final String column;
        private final String direction;

        private SortRow(HBox container, String column, String direction, Button removeButton) {
            this.container = container;
            this.column = column;
            this.direction = direction;
            this.columnLabel = new Label(column);
            this.directionLabel = new Label(direction);
            this.removeButton = removeButton;
        }
    }

    private static class AggregateFunction {
        private final String function;
        private final String column;
        private final String alias;

        private AggregateFunction(String function, String column, String alias) {
            this.function = function;
            this.column = column;
            this.alias = alias;
        }

        public String toSQL() {
            String sql = function + "(" + column + ")";
            if (alias != null && !alias.isBlank()) {
                sql += " AS " + alias;
            }
            return sql;
        }
    }

    private static class JoinRow {
        private final VBox container;
        private final String joinType;
        private final String rightTable;
        private final String rightAlias;
        private final String leftColumn;
        private final String rightColumn;

        private JoinRow(VBox container, String joinType, String rightTable, String rightAlias,
                        String leftColumn, String rightColumn) {
            this.container = container;
            this.joinType = joinType;
            this.rightTable = rightTable;
            this.rightAlias = rightAlias;
            this.leftColumn = leftColumn;
            this.rightColumn = rightColumn;
        }

        public String toSQL(String leftTableAlias) {
            String rightRef = (rightAlias != null && !rightAlias.isBlank()) ? rightAlias : rightTable;
            String quotedLeftAlias = quoteIdentStatic(leftTableAlias);
            String quotedRightRef = quoteIdentStatic(rightRef);
            String onClause = quotedLeftAlias + "." + quoteIdentStatic(leftColumn) + 
                              " = " + quotedRightRef + "." + quoteIdentStatic(rightColumn);
            return joinType + " " + quoteIdentStatic(rightTable) + 
                   ((rightAlias != null && !rightAlias.isBlank()) ? " AS " + quoteIdentStatic(rightAlias) : "") +
                   " ON " + onClause;
        }
    }

    /** Static version of quoteIdent for use in static inner classes */
    private static String quoteIdentStatic(String identifier) {
        if (identifier == null || identifier.isBlank()) {
            return identifier;
        }
        // Don't double-quote if already quoted
        if (identifier.startsWith("\"") && identifier.endsWith("\"")) {
            return identifier;
        }
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    // ========== COMPUTED COLUMN (CASE EXPRESSION) ==========
    private static class ComputedColumn {
        private final String alias;
        private final List<CaseWhen> caseWhens;
        private final String elseValue;

        private ComputedColumn(String alias, List<CaseWhen> caseWhens, String elseValue) {
            this.alias = alias;
            this.caseWhens = caseWhens;
            this.elseValue = elseValue;
        }

        public String toSQL() {
            StringBuilder sql = new StringBuilder("CASE");
            for (CaseWhen cw : caseWhens) {
                sql.append(" WHEN ").append(cw.condition).append(" THEN ").append(cw.result);
            }
            if (elseValue != null && !elseValue.isBlank()) {
                sql.append(" ELSE ").append(elseValue);
            }
            sql.append(" END");
            if (alias != null && !alias.isBlank()) {
                sql.append(" AS ").append(alias);
            }
            return sql.toString();
        }
    }

    private static class CaseWhen {
        private final String condition;
        private final String result;

        private CaseWhen(String condition, String result) {
            this.condition = condition;
            this.result = result;
        }
    }

    // ========== SUBQUERY FILTER ==========
    private static class SubqueryFilter {
        private final String column;
        private final String operator; // ANY, ALL, EXISTS
        private final String subqueryTable;
        private final String subqueryColumn;
        private final List<String> subqueryConditions;
        private final boolean isCorrelated;
        private final String correlationColumn;

        private SubqueryFilter(String column, String operator, String subqueryTable, 
                               String subqueryColumn, List<String> subqueryConditions,
                               boolean isCorrelated, String correlationColumn) {
            this.column = column;
            this.operator = operator;
            this.subqueryTable = subqueryTable;
            this.subqueryColumn = subqueryColumn;
            this.subqueryConditions = subqueryConditions;
            this.isCorrelated = isCorrelated;
            this.correlationColumn = correlationColumn;
        }

        public String toSQL(String mainTableAlias) {
            StringBuilder sql = new StringBuilder();
            StringBuilder subquery = new StringBuilder();
            subquery.append("SELECT ").append(quoteIdentStatic(subqueryColumn))
                    .append(" FROM ").append(quoteIdentStatic(subqueryTable));
            
            List<String> conditions = new ArrayList<>(subqueryConditions);
            if (isCorrelated && correlationColumn != null && !correlationColumn.isBlank()) {
                conditions.add(quoteIdentStatic(subqueryTable) + "." + quoteIdentStatic(correlationColumn) + 
                               " = " + quoteIdentStatic(mainTableAlias) + "." + quoteIdentStatic(correlationColumn));
            }
            
            if (!conditions.isEmpty()) {
                subquery.append(" WHERE ").append(String.join(" AND ", conditions));
            }
            
            if ("EXISTS".equals(operator)) {
                sql.append("EXISTS (").append(subquery).append(")");
            } else if ("NOT EXISTS".equals(operator)) {
                sql.append("NOT EXISTS (").append(subquery).append(")");
            } else if ("ANY".equals(operator)) {
                sql.append(quoteIdentStatic(column)).append(" = ANY (").append(subquery).append(")");
            } else if ("ALL".equals(operator)) {
                sql.append(quoteIdentStatic(column)).append(" = ALL (").append(subquery).append(")");
            } else {
                sql.append(quoteIdentStatic(column)).append(" IN (").append(subquery).append(")");
            }
            
            return sql.toString();
        }
    }

    // ========== NULL FUNCTION EXPRESSION (for future enhancements) ==========
    @SuppressWarnings("unused")
    private static class NullFunction {
        private final String functionType; // COALESCE or NULLIF
        private final String column;
        private final String value;
        private final String alias;

        private NullFunction(String functionType, String column, String value, String alias) {
            this.functionType = functionType;
            this.column = column;
            this.value = value;
            this.alias = alias;
        }

        public String toSQL() {
            String quotedColumn = quoteIdentStatic(column);
            String sql;
            if ("COALESCE".equals(functionType)) {
                sql = "COALESCE(" + quotedColumn + ", " + value + ")";
            } else {
                sql = "NULLIF(" + quotedColumn + ", " + value + ")";
            }
            if (alias != null && !alias.isBlank()) {
                sql += " AS " + quoteIdentStatic(alias);
            }
            return sql;
        }
    }

    // ========== COMPOSITE TYPE FIELD (for future enhancements) ==========
    @SuppressWarnings("unused")
    private static class CompositeTypeField {
        private final String name;
        private final String type;

        private CompositeTypeField(String name, String type) {
            this.name = name;
            this.type = type;
        }
    }
}
