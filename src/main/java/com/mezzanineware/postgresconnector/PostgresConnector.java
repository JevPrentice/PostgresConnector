package com.mezzanineware.postgresconnector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Tranquility
 */
public class PostgresConnector {

    /**
     * Singleton Instance
     */
    private static final PostgresConnector singleton = new PostgresConnector();

    /**
     * Checks Postgres Driver
     */
    private PostgresConnector() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(PostgresConnector.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(PostgresConnector.class.getName()).log(Level.INFO, "JDBC Driver is working.");

    }

    /**
     * Get the singleton instance
     *
     * @return
     */
    private static PostgresConnector getInstance() {
        return singleton;
    }

    /**
     * Get a connection using config file Remember to Close this Connection once
     * done!
     *
     * @param configFile
     * @return Connection or null if unable to create connection
     */
    private Connection getConnection(String configFile) {
        return singleton.createConnection(configFile);
    }

    /**
     * Create the connection using config file
     *
     * @param configFile
     * @return Connection or null if unable to create connection
     */
    private Connection createConnection(String configFile) {
        Connection connection = null;
        try {
            Properties properties = getProperties(configFile);
            String database = properties.getProperty("database_url");
            connection = DriverManager.getConnection(database, properties.getProperty("database_user"), properties.getProperty("database_password"));
            Logger.getLogger(PostgresConnector.class.getName()).log(Level.INFO, "Connection created to {0}", database);
        } catch (SQLException e) {
            Logger.getLogger(PostgresConnector.class.getName()).log(Level.SEVERE, "SQL Exception while trying to create database connection", e);
        } catch (IOException ex) {
            Logger.getLogger(PostgresConnector.class.getName()).log(Level.SEVERE, "Unable to access config file", ex);
        }
        return connection;
    }

    /**
     * Load the properties from given file
     *
     * @param configFile
     * @return
     * @throws IOException
     */
    private static Properties getProperties(String configFile) throws IOException {

        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(configFile));
        } catch (IOException e) {
            Logger.getLogger(PostgresConnector.class.getName()).log(Level.SEVERE, "Unable to load properties from: " + System.getProperty("user.dir") + "/" + configFile, e);
            throw new IOException(e);
        }
        return properties;
    }

    /**
     * Load SQL file into memory
     *
     * @param properties
     * @return
     * @throws FileNotFoundException
     */
    private static String getSqlFileText(Properties properties) throws FileNotFoundException {
        String pathname = properties.getProperty("sql_filename");

        File file = new File(pathname);
        if (!file.exists()) {
            FileNotFoundException e = new FileNotFoundException();
            Logger.getLogger(PostgresConnector.class.getName()).log(Level.SEVERE, "The file: " + System.getProperty("user.dir") + "/" + pathname + " does not exist.\nAborting Now.", e);
            throw e;
        }

        StringBuilder fileContents = new StringBuilder((int) file.length());
        try (Scanner scanner = new Scanner(file)) {
            while (scanner.hasNextLine()) {
                fileContents.append(scanner.nextLine()).append(System.getProperty("line.separator"));
            }
            return fileContents.toString();
        }
    }

    /**
     * Execute SQL file, using given connection and config file, into the
     * destination table
     *
     * @param connection
     * @param configFile
     * @throws SQLException
     * @throws FileNotFoundException
     */
    private void performQuery(Connection connection, String configFile) throws SQLException, FileNotFoundException {

        DatabaseMetaData metadata = connection.getMetaData();

        Properties properties;
        try {
            properties = getProperties(configFile);
        } catch (Exception e) {
            Logger.getLogger(PostgresConnector.class.getName()).log(Level.SEVERE, "Unable to access config file", e);
            return;
        }

        String tableName = properties.getProperty("destinationTable");
        String schemaName = properties.getProperty("destinationSchema");

        ResultSet tables = metadata.getTables(null, null, tableName, null);
        if (!tables.next()) {
            Logger.getLogger(PostgresConnector.class.getName()).log(Level.INFO, "The table {0}.{1} does not exist. \nAborting Now.", new Object[]{schemaName, tableName});
            return;
        }

        try (Statement stmt = connection.createStatement()) {
            if (properties.getProperty("b_do_truncate_export_table").equals("true")) {
                String truncateSql = "TRUNCATE " + schemaName + "." + tableName;
                stmt.executeUpdate(truncateSql);
                Logger.getLogger(PostgresConnector.class.getName()).log(Level.INFO, "{0} - successful", truncateSql);
            }

            String selectSql = getSqlFileText(properties);

            if (properties.getProperty("b_print_sql").equals("true")) {
                Logger.getLogger(PostgresConnector.class.getName()).log(Level.INFO, "Now Executing the following SQL:\n***** SQL START *****\n{0}\n***** SQL END *****", selectSql);
            }

            StringBuilder columns;
            StringBuilder values;
            ArrayList<String> column_values = new ArrayList();
            ArrayList<ArrayList> queryResultList;
            try (ResultSet rs = stmt.executeQuery(selectSql)) {

                ResultSetMetaData rsmd = rs.getMetaData();

                int columnsNumber = rsmd.getColumnCount();
                Logger.getLogger(PostgresConnector.class.getName()).log(Level.INFO, "Number Columns returned: {0}", columnsNumber);

                if (columnsNumber <= 0) {
                    Logger.getLogger(PostgresConnector.class.getName()).log(Level.INFO, "There were no columns returned by the query.");
                    return;
                } else if (columnsNumber > 9) {
                    Logger.getLogger(PostgresConnector.class.getName()).log(Level.INFO, "This query returns {0} columns, but the MAX is 9. :(", columnsNumber);
                    return;
                }

                columns = new StringBuilder();
                values = new StringBuilder();
                queryResultList = new ArrayList();
                for (int i = 1; rs.next(); i++) {

                    ArrayList<String> queryResultElement = new ArrayList();

                    for (int j = 1; j <= columnsNumber; j++) {

                        queryResultElement.add(rs.getString(j));

                        /* For the first record of resultset, get column and value string for INSERT sql */
                        if (i == 1) {
                            columns.append("column").append(j);
                            values.append("?");

                            column_values.add(rsmd.getColumnName(j));

                            if (j <= columnsNumber - 1) {
                                columns.append(", ");
                                values.append(", ");
                            }
                        }

                    }

                    queryResultList.add(queryResultElement);
                }
            }

            if (properties.getProperty("b_print_results").equals("true")) {
                Logger.getLogger(PostgresConnector.class.getName()).log(Level.INFO, "Query Result List: {0}", queryResultList.toString());
            }

            if (queryResultList.size() <= 0) {
                Logger.getLogger(PostgresConnector.class.getName()).log(Level.INFO, "Query Returned No Results");
                return;
            }

            if (properties.getProperty("b_insert_query_header").equals("true")) {
                queryResultList.add(0, column_values);
                Logger.getLogger(PostgresConnector.class.getName()).log(Level.INFO, "Inserting Query Headers into destination table first record.");
            }

            String insertSql = "INSERT INTO " + schemaName + "." + tableName + " (" + columns + ") VALUES (" + values + ")";

            PreparedStatement preparedStatement = connection.prepareStatement(insertSql);
            connection.setAutoCommit(false);

            int i = 0;
            for (ArrayList<String> queryRecord : queryResultList) {
                int j = 1;
                for (String s : queryRecord) {
                    preparedStatement.setString(j, s);
                    j++;
                }
                preparedStatement.addBatch();
                i++;
            }

            preparedStatement.executeBatch();
            connection.commit();

            Logger.getLogger(PostgresConnector.class.getName()).log(Level.INFO, "{0} - successful {1} record(s) inserted", new Object[]{insertSql, i});
        }
    }

    public static void main(String[] args) {

        Logger.getLogger(PostgresConnector.class.getName()).log(Level.INFO, "PostgresConnector Starting");

        String configFile = (args != null && args.length == 1) ? args[0] : "config.properties";
        Logger.getLogger(PostgresConnector.class.getName()).log(Level.INFO, "Using config file: {0}/{1}", new Object[]{System.getProperty("user.dir"), configFile});

        PostgresConnector instance = PostgresConnector.getInstance();

        if (instance == null) {
            Logger.getLogger(PostgresConnector.class.getName()).log(Level.SEVERE, "Unable to get an instance of singleton class.");
            return;
        }

        try (Connection connection = instance.getConnection(configFile)) {

            if (connection == null) {
                return;
            }

            instance.performQuery(connection, configFile);
            Logger.getLogger(PostgresConnector.class.getName()).log(Level.INFO, "Query Performed.");

        } catch (FileNotFoundException e) {
            Logger.getLogger(PostgresConnector.class.getName()).log(Level.SEVERE, "Failed to while reading .SQL file into memory.", e);
        } catch (SQLException e) {
            Logger.getLogger(PostgresConnector.class.getName()).log(Level.SEVERE, "SQL Exception while using database connection", e);
        }

        Logger.getLogger(PostgresConnector.class.getName()).log(Level.INFO, "PostgresConnector Finished");
    }
}
