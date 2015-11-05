package com.mezzanineware.postgresconnector;

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

        System.out.println("PostgresConnector Starting");

        String configFile = "config.properties";

        Singleton singleton;

        try {
            if (args != null && args.length == 1) {
                configFile = args[0];
            }

            System.out.println("Using config file: " + System.getProperty("user.dir") + "/" + configFile);

            singleton = Singleton.getInstance(configFile);

        } catch (IOException | SQLException e) {
            System.out.println("Unable to understand parameter, will now try to use default 'config.properties' file");
            Logger.getLogger(PostgresConnector.class.getName()).log(Level.SEVERE, null, e);
            return;
        }

        if (singleton == null) {
            System.out.println("Unable to get an instance of singleton class.");
            return;
        }

        try (Connection connection = Singleton.getConnection()) {

            if (connection == null) {
                return;
            }

            singleton.performQuery(connection);
            System.out.println("Query Performed.");

        } catch (IOException | SQLException e) {
            Logger.getLogger(PostgresConnector.class.getName()).log(Level.SEVERE, null, e);
        }

        System.out.println(
                "PostgresConnector Finished");
    }
}
