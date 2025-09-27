# DB Schema Studio

A desktop client built with JavaFX and Spring Boot for inspecting and editing PostgreSQL schemas. It lets you connect to a database, create tables, explore and filter table data, insert rows with or without transactions, rollback ongoing work, and review a live SQL operation log.

## Features

- **Connection status panel** – one-click connectivity health check with friendly feedback.
- **Table manager** – create new tables via structured column definitions and browse existing tables.
- **Data viewer** – dynamically renders table contents with ad-hoc filters and common comparison operators.
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
   - Update `src/main/resources/application.properties` with your PostgreSQL connection details (URL, username, password). Use the Docker Compose credentials above if you launched the local container.
   - Ensure the target database allows connections from this client.
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
- Use the **Tables** tab to create new tables. Provide column definitions line-by-line using standard SQL fragments (e.g. `id SERIAL PRIMARY KEY`).
- Switch to **Data Viewer** to inspect table contents. Pick a column, operator, and value to apply quick filters.
- In **Insert Data**, choose a table to auto-generate an input form. Mark *Use Transaction* to stage multiple inserts and rely on **Commit** / **Rollback** controls.
- The **SQL Log** pane reflects every executed SQL statement along with parameter bindings and runtime errors.

## Troubleshooting

- If the connection check fails, verify network reachability, credentials, and that PostgreSQL is running.
- For SSL-secured databases, extend `application.properties` with the relevant `spring.datasource.hikari.*` SSL options.
- When running the shaded JAR, ensure `libopenjfx` is available on your system; on some Linux distributions you may need to export `PATH_TO_FX` and add it to the `java` command.

## Development Notes

- The JavaFX UI and the Spring context are integrated through `DbSchemaStudioFxApplication`, letting controllers consume Spring-managed services.
- Long-running database interactions execute on a dedicated background executor. UI updates are marshalled back to the JavaFX Application Thread.
- SQL statements run through a centralized `DatabaseService`, which also maintains manual transaction handles for the rollback workflow.

## License

This project is provided as-is for demonstration purposes.
