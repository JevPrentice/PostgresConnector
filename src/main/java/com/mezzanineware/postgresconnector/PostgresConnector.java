package com.mezzanineware.postgresconnector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.Connection;
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
 * @author Jev Prentice
 * @since 2015
 *
 * Connect to database Read SQL (sql_filename)
 *
 * Execute and persist output of query to database in table
 * destinationTable.destinationSchema
 */
public class PostgresConnector {

    /**
     * Singleton Instance
     */
    final static private PostgresConnector SINGLETON = new PostgresConnector();
    final static private Logger LOGGER = Logger.getLogger(PostgresConnector.class.getName());

    /**
     * Checks PostgreSQL Driver
     */
    private PostgresConnector() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (final ClassNotFoundException ex) {
            LOGGER.log(Level.SEVERE, "Missing PostgreSQL Driver", ex);
        }
        LOGGER.log(Level.INFO, "JDBC Driver is working");
    }

    /**
     * Get the singleton instance
     *
     * @return
     */
    private static PostgresConnector getInstance() {
        return SINGLETON;
    }

    /**
     * Get a connection using configFile
     *
     * Remember to Close this Connection!
     *
     * @param configFile
     */
    private Connection getConnection(final String configFile) {
        try {
            final Properties properties = getProperties(configFile);
            return DriverManager.getConnection(
                    properties.getProperty("database_url"),
                    properties.getProperty("database_user"),
                    properties.getProperty("database_password"));
        } catch (final SQLException e) {
            throw new RuntimeException("SQL Exception while trying to create "
                    + "database connection check configFile values of "
                    + "database_url, database_user and database_password", e);
        }
    }

    /**
     * Load the properties from configFile
     *
     * @param configFile
     * @return Properties
     */
    private static Properties getProperties(final String configFile) {
        try {
            final Properties properties = new Properties();
            properties.load(new FileInputStream(configFile));
            return properties;
        } catch (final IOException e) {
            throw new RuntimeException(String.format(
                    "Unable to load properties from: %s/%s Exception: %s",
                    System.getProperty("user.dir"), configFile, e));
        }
    }

    /**
     * Load SQL file into memory
     *
     * @param properties
     * @return
     */
    private static String getSqlFileText(final Properties properties) {
        final String pathname = properties.getProperty("sql_filename");
        final StringBuilder fileContents;
        final File file;
        try {
            file = new File(pathname);
            fileContents = new StringBuilder((int) file.length());
        } catch (final NullPointerException e) {
            LOGGER.log(Level.SEVERE, "Missing or invalid or value in config.properties for sql_filename", e);
            throw (e);
        }
        try (final Scanner scanner = new Scanner(file)) {
            while (scanner.hasNextLine()) {
                fileContents.append(scanner.nextLine()).append(System.getProperty("line.separator"));
            }
            return fileContents.toString();
        } catch (final FileNotFoundException e) {
            throw new RuntimeException(String.format("The file: %s/%s does not exist - Aborting Now.", System.getProperty("user.dir"), pathname), e);
        }
    }

    /**
     * Execute SQL file, using given connection and config file, into the
     * destination table
     *
     * @param connection
     * @param configFile
     * @throws SQLException
     */
    private void performQuery(final Connection connection, final String configFile) throws SQLException {

        final Properties properties = getProperties(configFile);
        final String tableName = properties.getProperty("destinationTable");
        final String schemaName = properties.getProperty("destinationSchema");

        if (!connection.getMetaData().getTables(null, null, tableName, null).next()) {
            throw new RuntimeException(String.format(
                    "Table: %s.%s does not exist. - Aborting Now.",
                    schemaName, tableName));
        }

        try (final Statement stmt = connection.createStatement()) {

            if (properties.getProperty("b_do_truncate_export_table").equals("true")) {
                final String truncateSql = "TRUNCATE " + schemaName + "." + tableName;
                stmt.executeUpdate(truncateSql);
                LOGGER.log(Level.INFO, "{0} - successful", truncateSql);
            }

            final String selectSql = getSqlFileText(properties);

            if (properties.getProperty("b_print_sql").equals("true")) {
                LOGGER.log(Level.INFO, String.format("Now Executing the following SQL:\n***** SQL START *****\n%s\n***** SQL END *****", selectSql));
            }

            final StringBuilder columns;
            final StringBuilder values;
            final ArrayList<String> column_values = new ArrayList();
            final ArrayList<ArrayList> queryResultList;
            try (final ResultSet rs = stmt.executeQuery(selectSql)) {

                final ResultSetMetaData rsmd = rs.getMetaData();
                final int columnsNumber = rsmd.getColumnCount();
                LOGGER.log(Level.INFO, String.format("Number Columns returned: %s", columnsNumber));

                if (columnsNumber <= 0) {
                    LOGGER.log(Level.WARNING, "There were no columns returned by the query.");
                    return;
                } else if (columnsNumber > 9) {
                    LOGGER.log(Level.WARNING, String.format("This query returns %s columns, but the MAX is 9. :(", columnsNumber));
                    return;
                }

                columns = new StringBuilder();
                values = new StringBuilder();
                queryResultList = new ArrayList();
                for (int i = 1; rs.next(); i++) {

                    final ArrayList<String> queryResultElement = new ArrayList();

                    for (int j = 1; j <= columnsNumber; j++) {

                        queryResultElement.add(rs.getString(j));

                        /* For the first record of resultset, get column and value string for INSERT sql */
                        if (i == 1) {
                            columns.append("column").append(j);
                            values.append("?");

                            if (j <= columnsNumber - 1) {
                                columns.append(", ");
                                values.append(", ");
                            }

                            column_values.add(rsmd.getColumnName(j));

                        }

                    }

                    queryResultList.add(queryResultElement);
                }
            }

            if (properties.getProperty("b_print_results").equals("true")) {
                LOGGER.log(Level.INFO, String.format("Query Result List: %s", queryResultList.toString()));
            }

            if (queryResultList.size() <= 0) {
                throw new RuntimeException("Query Returned No Results");
            }

            if (properties.getProperty("b_insert_query_header").equals("true")) {
                queryResultList.add(0, column_values);
                LOGGER.log(Level.INFO, "Inserting Query Headers into destination table first record.");
            }

            final String insertSql = String.format("INSERT INTO %s.%s (%s) VALUES (%s)", schemaName, tableName, columns, values);

            final PreparedStatement preparedStatement = connection.prepareStatement(insertSql);
            connection.setAutoCommit(false);

            int i = 0;
            for (final ArrayList<String> queryRecord : queryResultList) {
                int j = 1;
                for (final String s : queryRecord) {
                    preparedStatement.setString(j, s);
                    j++;
                }
                preparedStatement.addBatch();
                i++;
            }

            preparedStatement.executeBatch();
            connection.commit();

            LOGGER.log(Level.INFO, String.format("%s - successful %s record(s) inserted", insertSql, i));
        }
    }

    public static void main(final String[] args) {

        LOGGER.log(Level.INFO, "PostgresConnector Starting");

        final String configFile = (args != null && args.length == 1) ? args[0] : "config.properties";
        LOGGER.log(Level.INFO, String.format("Using config file: %s/%s", System.getProperty("user.dir"), configFile));

        final PostgresConnector instance = PostgresConnector.getInstance();
        try (final Connection connection = instance.getConnection(configFile)) {
            instance.performQuery(connection, configFile);
        } catch (final SQLException e) {
            throw new RuntimeException("SQL Exception while using database connection", e);
        }

        LOGGER.log(Level.INFO, "PostgresConnector Finished");
    }
}
