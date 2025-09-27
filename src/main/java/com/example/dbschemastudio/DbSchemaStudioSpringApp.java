package com.example.dbschemastudio;

import javafx.application.Application;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DbSchemaStudioSpringApp {

    public static void main(String[] args) {
        Application.launch(DbSchemaStudioFxApplication.class, args);
    }
}
