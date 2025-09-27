package com.example.dbschemastudio.controller;

import com.example.dbschemastudio.model.ConnectionSettings;
import javafx.application.Platform;
import javafx.beans.binding.BooleanBinding;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.value.ChangeListener;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.control.TextField;
import javafx.scene.control.TextFormatter.Change;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

public class ConnectionDialogController {

    @FXML
    private TextField hostField;
    @FXML
    private TextField portField;
    @FXML
    private TextField databaseField;
    @FXML
    private TextField usernameField;
    @FXML
    private PasswordField passwordField;
    @FXML
    private TextField schemaField;
    @FXML
    private CheckBox rememberCheckBox;
    @FXML
    private Button testConnectionButton;
    @FXML
    private Button connectButton;
    @FXML
    private Button cancelButton;
    @FXML
    private Label testStatusLabel;
    @FXML
    private ProgressIndicator activityIndicator;

    private ConnectionSettings settings = ConnectionSettings.defaults();
    private ConnectionSettings lastVerifiedSettings;

    private final BooleanProperty connectionVerified = new SimpleBooleanProperty(false);
    private final BooleanProperty busy = new SimpleBooleanProperty(false);

    private Consumer<ConnectionSettings> onConnect;
    private Runnable onCancel;

    public void initialize() {
        UnaryOperator<Change> integerFilter = change -> {
            String text = change.getControlNewText();
            if (text.matches("\\d{0,5}")) {
                return change;
            }
            return null;
        };
        portField.setTextFormatter(new javafx.scene.control.TextFormatter<>(integerFilter));

        BooleanBinding requiredFieldsEmpty = hostField.textProperty().isEmpty()
                .or(portField.textProperty().isEmpty())
                .or(databaseField.textProperty().isEmpty())
                .or(usernameField.textProperty().isEmpty());

        connectButton.disableProperty().bind(busy.or(requiredFieldsEmpty).or(connectionVerified.not()));
        testConnectionButton.disableProperty().bind(busy);
        cancelButton.disableProperty().bind(busy);

        hostField.disableProperty().bind(busy);
        portField.disableProperty().bind(busy);
        databaseField.disableProperty().bind(busy);
        usernameField.disableProperty().bind(busy);
        passwordField.disableProperty().bind(busy);
        schemaField.disableProperty().bind(busy);
        rememberCheckBox.disableProperty().bind(busy);
        activityIndicator.visibleProperty().bind(busy);
        activityIndicator.managedProperty().bind(busy);

        ChangeListener<String> resetListener = (obs, oldVal, newVal) -> resetVerification();
        hostField.textProperty().addListener(resetListener);
        portField.textProperty().addListener(resetListener);
        databaseField.textProperty().addListener(resetListener);
        usernameField.textProperty().addListener(resetListener);
        passwordField.textProperty().addListener(resetListener);
        schemaField.textProperty().addListener(resetListener);

        testConnectionButton.setOnAction(event -> testConnection());
        connectButton.setOnAction(event -> {
            if (onConnect != null && connectionVerified.get()) {
                onConnect.accept(connectionSettingsForConnect());
            }
        });
        cancelButton.setOnAction(event -> {
            if (!busy.get() && onCancel != null) {
                onCancel.run();
            }
        });

        resetVerification();
        apply(settings);

        Platform.runLater(hostField::requestFocus);
    }

    public void setSettings(ConnectionSettings settings) {
        this.settings = settings;
        if (hostField != null) {
            apply(settings);
        }
    }

    public void setOnConnect(Consumer<ConnectionSettings> onConnect) {
        this.onConnect = onConnect;
    }

    public void setOnCancel(Runnable onCancel) {
        this.onCancel = onCancel;
    }

    public void setBusy(boolean busy, String message) {
        this.busy.set(busy);
        if (busy && message != null) {
            showInfo(message);
        }
    }

    public boolean isBusy() {
        return busy.get();
    }

    public void showError(String message) {
        setStatus(message, StatusType.ERROR);
    }

    public void showInfo(String message) {
        setStatus(message, StatusType.INFO);
    }

    public void showSuccess(String message) {
        setStatus(message, StatusType.SUCCESS);
    }

    private void apply(ConnectionSettings settings) {
        hostField.setText(settings.host());
        portField.setText(String.valueOf(settings.port()));
        databaseField.setText(settings.database());
        usernameField.setText(settings.username());
        passwordField.setText(settings.password());
        schemaField.setText(settings.schema());
        rememberCheckBox.setSelected(settings.rememberSession());
    }

    private void testConnection() {
        ConnectionSettings snapshot = snapshotFromFields(true);

        connectionVerified.set(false);
        lastVerifiedSettings = null;
        setBusy(true, "Testing connection...");

        CompletableFuture
                .supplyAsync(() -> attemptConnection(snapshot))
                .whenComplete((success, throwable) -> Platform.runLater(() -> {
                    setBusy(false, null);
                    if (throwable != null) {
                        showError(rootMessage(throwable));
                        connectionVerified.set(false);
                        lastVerifiedSettings = null;
                    } else if (Boolean.TRUE.equals(success)) {
                        showSuccess("Connection successful.");
                        connectionVerified.set(true);
                        lastVerifiedSettings = snapshot;
                    } else {
                        showError("Unable to connect.");
                        connectionVerified.set(false);
                        lastVerifiedSettings = null;
                    }
                }));
    }

    private boolean attemptConnection(ConnectionSettings snapshot) {
        ensureDriverRegistered();
        try (Connection connection = DriverManager.getConnection(buildJdbcUrl(snapshot), buildProperties(snapshot))) {
            String schema = snapshot.schema();
            if (schema != null && !schema.isBlank()) {
                try {
                    connection.setSchema(schema);
                } catch (SQLException ignored) {
                    // Ignore optional schema switching failures during validation
                }
            }
            return true;
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void ensureDriverRegistered() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException("PostgreSQL JDBC driver not found on classpath", ex);
        }
    }

    private Properties buildProperties(ConnectionSettings snapshot) {
        Properties props = new Properties();
        props.setProperty("user", snapshot.username());
        props.setProperty("password", snapshot.password());
        if (snapshot.schema() != null && !snapshot.schema().isBlank()) {
            props.setProperty("currentSchema", snapshot.schema());
        }
        if (isLocalHost(snapshot.host())) {
            props.setProperty("sslmode", "disable");
            props.setProperty("ssl", "false");
        }
        return props;
    }

    private String buildJdbcUrl(ConnectionSettings snapshot) {
        return "jdbc:postgresql://" + snapshot.host() + ":" + snapshot.port() + "/" + snapshot.database();
    }

    private boolean isLocalHost(String host) {
        if (host == null) {
            return false;
        }
        String normalized = host.trim().toLowerCase();
        return normalized.equals("localhost")
                || normalized.equals("127.0.0.1")
                || normalized.equals("::1");
    }

    private ConnectionSettings snapshotFromFields(boolean includeRememberFlag) {
        int port = settings.port();
        String portText = portField.getText().trim();
        if (!portText.isEmpty()) {
            port = Integer.parseInt(portText);
        }
        boolean remember = includeRememberFlag && rememberCheckBox.isSelected();
        String schema = schemaField.getText().isBlank() ? "public" : schemaField.getText().trim();
        return new ConnectionSettings(
                hostField.getText().trim(),
                port,
                databaseField.getText().trim(),
                usernameField.getText().trim(),
                passwordField.getText(),
                schema,
                remember
        );
    }

    private ConnectionSettings connectionSettingsForConnect() {
        if (!connectionVerified.get()) {
            throw new IllegalStateException("Connection must be tested successfully before continuing.");
        }
        ConnectionSettings base = lastVerifiedSettings != null ? lastVerifiedSettings : snapshotFromFields(true);
        if (base.rememberSession() != rememberCheckBox.isSelected()) {
            base = base.withRemembered(rememberCheckBox.isSelected());
        }
        return base;
    }

    private void resetVerification() {
        connectionVerified.set(false);
        lastVerifiedSettings = null;
        setStatus("Test connection to continue.", StatusType.INFO);
    }

    private void setStatus(String message, StatusType type) {
        testStatusLabel.setText(message);
        testStatusLabel.setStyle("-fx-text-fill: " + switch (type) {
            case SUCCESS -> "#2e7d32";
            case ERROR -> "#c62828";
            case INFO -> "#616161";
        } + ";");
    }

    private String rootMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        String message = current.getMessage();
        return (message == null || message.isBlank()) ? current.toString() : message;
    }

    private enum StatusType {
        INFO,
        SUCCESS,
        ERROR
    }
}
