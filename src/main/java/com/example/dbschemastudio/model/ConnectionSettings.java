package com.example.dbschemastudio.model;

public record ConnectionSettings(
        String host,
        int port,
        String database,
        String username,
        String password,
        String schema,
        boolean rememberSession
) {
    public static ConnectionSettings defaults() {
        return new ConnectionSettings("localhost", 5432, "dbschemastudio", "dbstudio", "dbstudio", "public", false);
    }

    public ConnectionSettings withRemembered(boolean remember) {
        return new ConnectionSettings(host, port, database, username, password, schema, remember);
    }

    public ConnectionSettings withoutPassword() {
        return new ConnectionSettings(host, port, database, username, "", schema, rememberSession);
    }
}
