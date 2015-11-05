package com.mezzanineware.postgresconnector;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author jevprentice
 */
public class PostgresConnector {

    public PostgresConnector() {
    }

    public static void main(String[] args) {

        Logger.getLogger(PostgresConnector.class.getName()).log(Level.INFO, "PostgresConnector Starting");

        String configFile = (args != null && args.length == 1) ? args[0] : "config.properties";
        Logger.getLogger(PostgresConnector.class.getName()).log(Level.INFO, "Using config file: {0}/{1}", new Object[]{System.getProperty("user.dir"), configFile});

        Singleton singleton = Singleton.getInstance(configFile);

        if (singleton == null) {
            Logger.getLogger(PostgresConnector.class.getName()).log(Level.SEVERE, "Unable to get an instance of singleton class.");
            return;
        }

        try (Connection connection = Singleton.getConnection()) {

            if (connection == null) {
                return;
            }

            singleton.performQuery(connection);
            Logger.getLogger(PostgresConnector.class.getName()).log(Level.INFO, "Query Performed.");

        } catch (FileNotFoundException e) {
            Logger.getLogger(PostgresConnector.class.getName()).log(Level.SEVERE, "Failed to while reading .SQL file into memory.", e);
        } catch (SQLException e) {
            Logger.getLogger(PostgresConnector.class.getName()).log(Level.SEVERE, "SQL Exception while using database connection", e);
        }

        Logger.getLogger(PostgresConnector.class.getName()).log(Level.INFO, "PostgresConnector Finished");

    }
}
