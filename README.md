# DB Schema Studio

A desktop client built with JavaFX and Spring Boot for inspecting and editing PostgreSQL schemas. It lets you connect to a database, create tables, explore and filter table data, insert rows with or without transactions, rollback ongoing work, and review a live SQL operation log.

## Features

- **Connection status panel** – one-click connectivity health check with friendly feedback.
- **ENUM type management** – create custom PostgreSQL ENUM types with comma-separated values and view existing types.
- **Table manager** – create new tables via a visual column editor with type selection, constraints, and ENUM support; browse existing tables.
- **Data viewer** – dynamically renders table contents with ad-hoc filters, lets you stack multiple conditions (combined with AND), and supports common comparison operators.
- **Data entry** – generate insert forms per table, enforce required columns, and submit rows instantly or within a long-running transaction.
- **Transaction controls** – begin, commit, or rollback transactions and reuse them across multiple operations.
- **SQL log & error surfacing** – see every executed statement, parameter payload, and any error captured in real time.

## Requirements

- Java 21 JDK
- Maven 3.9+
- Running PostgreSQL instance (any recent version)

## Setup

### Optional: Launch PostgreSQL locally with Docker Compose

If you don't already have a PostgreSQL instance handy, the included `docker-compose.yml` spins one up quickly:

```bash
docker compose up -d
```

This starts a container named `dbschema-postgres` on port `5432` with credentials `dbstudio/dbstudio` and database `dbschemastudio`. The volume `postgres-data` persists data between restarts.

### Application build & run

1. **Clone & configure**
   - Launch the app and use the connection dialog to supply host, port, database, credentials, and optional default schema. The form is pre-filled with the Docker Compose defaults when available.
   - If you need to hard-code defaults (for example, when running headless), set `spring.datasource.*` properties via environment variables or command-line arguments rather than editing `application.properties` directly.
2. **Build the application**
   ```bash
   mvn -DskipTests package
   ```
3. **Run the desktop app**
   ```bash
   mvn javafx:run
   ```
   Alternatively, execute the shaded JAR directly:
   ```bash
   java --enable-native-access=ALL-UNNAMED -jar target/db-schema-studio-0.0.1-SNAPSHOT.jar
   ```

## Usage Tips

- The application now opens with a dedicated connection window before the workspace loads. Enter the PostgreSQL host, port, database, credentials, and default schema there. The form is pre-filled with the Docker Compose defaults (`localhost:5432`, database `dbschemastudio`, user `dbstudio`, password `dbstudio`). Use **Test Connection** to validate the settings; the **Connect** button only enables after a successful check. Choose **Remember for session** to preload the same values next time during the same app run. When connecting to the bundled Docker Compose database, SSL is automatically disabled to avoid EOF/handshake errors; provide your own SSL parameters if targeting a secure instance.
- Use the **ENUM Types** section to create custom PostgreSQL ENUM types. Enter a name and comma-separated values (e.g., `pending,active,done`), then click **Create ENUM**. Double-click any enum in the list to view its values. These ENUMs become available in the type dropdown when creating tables.
- Use the **Tables** tab to create new tables with the visual column editor. Click **Add Column** to add rows specifying name, type (with auto-complete for common types and ENUMs), and optional constraints like `NOT NULL` or `PRIMARY KEY`. Click **Create Table** when done, or **Clear** to reset the form.
- Switch to **Data Viewer** to inspect table contents. Use **Add Filter** to stack multiple conditions (combined with AND); choose a column, operator, and value for each clause, then press **Apply**.
- In **Insert Data**, choose a table to auto-generate an input form. Mark *Use Transaction* to stage multiple inserts and rely on **Commit** / **Rollback** controls.
- The **SQL Log** pane reflects every executed SQL statement along with parameter bindings and runtime errors.

## Troubleshooting

- If the connection check fails, verify network reachability, credentials, and that PostgreSQL is running.
- For SSL-secured databases, provide the required `spring.datasource.hikari.*` SSL options via environment variables or command-line overrides when launching the app.
- When running the shaded JAR, ensure `libopenjfx` is available on your system; on some Linux distributions you may need to export `PATH_TO_FX` and add it to the `java` command.

## Development Notes

- The JavaFX UI and the Spring context are integrated through `DbSchemaStudioFxApplication`, letting controllers consume Spring-managed services.
- Long-running database interactions execute on a dedicated background executor. UI updates are marshalled back to the JavaFX Application Thread.
- SQL statements run through a centralized `DatabaseService`, which also maintains manual transaction handles for the rollback workflow.

## License

This project is provided as-is for demonstration purposes.
