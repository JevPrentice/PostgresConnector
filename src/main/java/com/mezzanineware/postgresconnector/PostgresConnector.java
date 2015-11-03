package com.mezzanineware.postgresconnector;

import java.io.File;
import java.io.FileInputStream;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

/**
 *
 * @author jevprentice
 */
public class PostgresConnector {

    Properties properties;

    public PostgresConnector(String configFile) {
        this.properties = getProperties(configFile);
    }

    private static Properties getProperties(String filename) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(filename));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return properties;
    }

    private static String readFile(String pathname) throws IOException {

        File file = new File(pathname);

        if (!file.exists()) {
            System.out.println("The file: " + System.getProperty("user.dir") + "/" + pathname + " does not exist.\nAborting Now.");
        }

        StringBuilder fileContents = new StringBuilder((int) file.length());
        Scanner scanner = new Scanner(file);
        String lineSeparator = System.getProperty("line.separator");

        try {
            while (scanner.hasNextLine()) {
                fileContents.append(scanner.nextLine()).append(lineSeparator);
            }
            return fileContents.toString();
        } finally {
            scanner.close();
        }
    }

    private void performQuery(Connection connection) {

        ResultSet rs = null;
        Statement stmt = null;
        PreparedStatement preparedStatement;
        ResultSetMetaData rsmd;
        int columnsNumber;

        String tableName = properties.getProperty("destinationTable");
        String schemaName = properties.getProperty("destinationSchema");
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();
        String insertSql;

        try {
            stmt = connection.createStatement();

            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet tables = metadata.getTables(null, null, tableName, null);
            if (!tables.next()) {
                System.out.println("The table " + schemaName + "." + tableName + " does not exist. \nAborting Now.");
                return;
            }

            if (properties.getProperty("b_do_truncate_export_table").equals("true")) {
                String truncateSql = "TRUNCATE " + schemaName + "." + tableName;
                stmt.executeUpdate(truncateSql);
                System.out.println(truncateSql + " - successful");
            }

            String selectSql = readFile(properties.getProperty("sql_filename"));

            if (properties.getProperty("b_print_sql").equals("true")) {
                System.out.println("Now Executing the following SQL:\n***** SQL START *****\n" + selectSql + "\n***** SQL END *****");
            }

            rs = stmt.executeQuery(selectSql);

            rsmd = rs.getMetaData();
            columnsNumber = rsmd.getColumnCount();

            System.out.println("Number Columns returned: " + columnsNumber);

            if (columnsNumber <= 0) {
                System.out.println("There were no columns returned by the query.");
                return;
            } else if (columnsNumber > 9) {
                System.out.println("This query returns " + columnsNumber + " columns, but the MAX is 9. :(");
                return;
            }

            ArrayList<ArrayList> queryResultList = new ArrayList();
            for (int i = 1; rs.next(); i++) {

                ArrayList<String> record = new ArrayList();

                for (int j = 1; j <= columnsNumber; j++) {
                    String columnValue = rs.getString(j);

                    record.add(columnValue);

                    if (i == 1) {
                        columns.append("column").append(j);
                        values.append("?");

                        if (j <= columnsNumber - 1) {
                            columns.append(", ");
                            values.append(", ");
                        }
                    }

                }

                queryResultList.add(record);
            }

            if (properties.getProperty("b_print_results").equals("true")) {
                System.out.println("Query Results: " + queryResultList.toString());
            }
            
            insertSql = "INSERT INTO " + schemaName + "." + tableName + " (" + columns + ") VALUES (" + values + ")";

            preparedStatement = connection.prepareStatement(insertSql);
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

            System.out.println(insertSql + " - successful " + i + " record(s) inserted");

        } catch (SQLException | IOException e) {
            e.printStackTrace();
        } finally {

            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }

            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }

        }

    }

    public static void main(String[] args) {

        System.out.println("PostgresConnector Starting");

        String configFileName = "config.properties";
        try {
            if (args != null && args.length == 1) {
                configFileName = args[0];
            }
        } catch (Exception e) {
            System.out.println("Unable to understand parameter, will now try to use default 'config.properties' file");
            e.printStackTrace();
        }

        System.out.println("Using config file: " + System.getProperty("user.dir") + "/" + configFileName);

        PostgresConnector pgConn = new PostgresConnector(configFileName);
        Connection connection = null;

        try {
            Class.forName(pgConn.properties.getProperty("database_driver"));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }

        try {

            final String databaseUrl = pgConn.properties.getProperty("database_url");
            final String databaseUser = pgConn.properties.getProperty("database_user");
            final String databasePassword = pgConn.properties.getProperty("database_password");

            connection = DriverManager.getConnection(databaseUrl, databaseUser, databasePassword);
            System.out.println("Connection created.");

            pgConn.performQuery(connection);
            System.out.println("Query Performed.");

            connection.close();
            System.out.println("Connection closed.");

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        System.out.println(
                "PostgresConnector Finished");
    }
}
