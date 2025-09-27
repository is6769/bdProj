package com.example.dbschemastudio;

import com.example.dbschemastudio.controller.ConnectionDialogController;
import com.example.dbschemastudio.model.ConnectionSettings;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.HashMap;
import java.util.Map;

public class DbSchemaStudioFxApplication extends Application {

    private ConfigurableApplicationContext context;
    private static ConnectionSettings rememberedSettings = ConnectionSettings.defaults();

    @Override
    public void start(Stage primaryStage) throws Exception {
        primaryStage.setTitle("DB Schema Studio");
        showConnectionScene(primaryStage);
    }

    @Override
    public void stop() {
        if (context != null) {
            context.close();
        }
        Platform.exit();
    }

    private void showConnectionScene(Stage stage) throws IOException {
        FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/connection-dialog.fxml"));
        Parent root = loader.load();
        ConnectionDialogController controller = loader.getController();
        controller.setSettings(rememberedSettings);
        controller.setOnConnect(settings -> attemptConnection(stage, controller, settings));
        controller.setOnCancel(Platform::exit);

        Scene scene = new Scene(root);
        var stylesheet = getClass().getResource("/application.css");
        if (stylesheet != null) {
            scene.getStylesheets().add(stylesheet.toExternalForm());
        }

        stage.setScene(scene);
        stage.centerOnScreen();
        stage.show();
        stage.setOnCloseRequest(event -> Platform.exit());
    }

    private void attemptConnection(Stage stage, ConnectionDialogController controller, ConnectionSettings settings) {
        if (controller.isBusy()) {
            return;
        }

        controller.setBusy(true, "Connecting to database...");

        CompletableFuture
                .supplyAsync(() -> startSpringContext(settings))
                .whenComplete((ctx, throwable) -> Platform.runLater(() -> {
                    if (throwable != null) {
                        controller.setBusy(false, null);
                        Throwable root = rootCause(throwable);
                        controller.showError(root.getMessage() != null ? root.getMessage() : root.toString());
                    } else {
                        if (context != null) {
                            context.close();
                        }
                        context = ctx;
                        rememberSettings(settings);
                        controller.showSuccess("Connection established.");
                        try {
                            launchMainStage(stage);
                        } catch (IOException ioException) {
                            controller.setBusy(false, null);
                            controller.showError(ioException.getMessage());
                        }
                    }
                }));
    }

    private void launchMainStage(Stage primaryStage) throws IOException {
        FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/main-view.fxml"));
        loader.setControllerFactory(context::getBean);
        Parent root = loader.load();

        Scene scene = new Scene(root);
        var stylesheet = getClass().getResource("/application.css");
        if (stylesheet != null) {
            scene.getStylesheets().add(stylesheet.toExternalForm());
        }

        primaryStage.setScene(scene);
        primaryStage.show();
    }

    private ConfigurableApplicationContext startSpringContext(ConnectionSettings settings) {
        Map<String, Object> props = new HashMap<>();
        props.put("spring.datasource.url", buildJdbcUrl(settings));
        props.put("spring.datasource.username", settings.username());
        props.put("spring.datasource.password", settings.password());
    props.put("spring.datasource.driver-class-name", "org.postgresql.Driver");
        props.put("spring.datasource.hikari.schema", settings.schema());
        props.put("spring.jpa.properties.hibernate.default_schema", settings.schema());
        props.put("spring.datasource.hikari.maximumPoolSize", "5");
        if (isLocalHost(settings.host())) {
            props.put("spring.datasource.hikari.dataSourceProperties.sslmode", "disable");
            props.put("spring.datasource.hikari.dataSourceProperties.ssl", "false");
        }

        ApplicationContextInitializer<ConfigurableApplicationContext> initializer = ctx ->
                ctx.getBeanFactory().registerSingleton("connectionSettings", settings);

        return new SpringApplicationBuilder(DbSchemaStudioSpringApp.class)
                .properties(props)
                .initializers(initializer)
                .run(getParameters().getRaw().toArray(new String[0]));
    }

    private String buildJdbcUrl(ConnectionSettings settings) {
        String base = "jdbc:postgresql://" + settings.host() + ":" + settings.port() + "/" + settings.database();
        if (isLocalHost(settings.host())) {
            return base + "?sslmode=disable";
        }
        return base;
    }

    private Throwable rootCause(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
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

    private void rememberSettings(ConnectionSettings settings) {
        if (settings.rememberSession()) {
            rememberedSettings = settings;
        } else {
            rememberedSettings = settings.withRemembered(false).withoutPassword();
        }
    }
}
