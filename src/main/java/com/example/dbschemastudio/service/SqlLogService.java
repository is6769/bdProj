package com.example.dbschemastudio.service;

import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Service
public class SqlLogService {
    private final ObservableList<String> entries = FXCollections.observableArrayList();
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final int MAX_ENTRIES = 500;

    public ObservableList<String> getEntries() {
        return entries;
    }

    public void logOperation(String sql) {
        append("SQL", sql);
    }

    public void logError(String sql, String message) {
        append("ERR", sql + " :: " + message);
    }

    public void logInfo(String message) {
        append("INF", message);
    }

    private void append(String prefix, String message) {
        String timestamp = LocalDateTime.now().format(formatter);
        String entry = String.format("%s %s %s", timestamp, prefix, message);
        if (Platform.isFxApplicationThread()) {
            entries.add(entry);
            trimIfNeeded();
        } else {
            Platform.runLater(() -> {
                entries.add(entry);
                trimIfNeeded();
            });
        }
    }

    private void trimIfNeeded() {
        if (entries.size() > MAX_ENTRIES) {
            entries.remove(0, entries.size() - MAX_ENTRIES);
        }
    }
}
