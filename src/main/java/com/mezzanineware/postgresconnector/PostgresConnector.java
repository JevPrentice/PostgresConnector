package com.mezzanineware.postgresconnector;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
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

    private void dumpMap(HashMap<String, String> hashMap) {

        System.out.println("***** DUMP MAP START *****");
        int i = 1;
        for (Map.Entry<String, String> entry : hashMap.entrySet()) {

            String key = entry.getKey();
            String value = entry.getValue();

            System.out.println("Element = " + i + " KEY = " + key + " VALUE " + value);
            i++;
        }
        System.out.println("***** DUMP MAP END *****");

    }

    private void performQuery(Connection connection) {

        ResultSet rs = null;
        Statement stmt = null;
        PreparedStatement preparedStatement;
        ResultSetMetaData rsmd;
        int columnsNumber;

        String tableName = properties.getProperty("exportToTableName");
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();
        String insertSql;

        try {

            stmt = connection.createStatement();

            String sql = readFile(properties.getProperty("sql_filename"));

            if (properties.getProperty("b_print_sql").equals("true")) {
                System.out.println("Now Executing the following SQL:\n***** SQL START *****\n" + sql + "\n***** SQL END *****");
            }

            rs = stmt.executeQuery(sql);

            rsmd = rs.getMetaData();
            columnsNumber = rsmd.getColumnCount();

            System.out.println("Number Results returned: " + columnsNumber);

            if (columnsNumber <= 0) {
                System.out.println("There were no results returned by the query.");
                return;
            } else if (columnsNumber > 9) {
                System.out.println("This query returns " + columnsNumber + " columns, but the MAX is 9. :(");
                return;
            }

            HashMap<String, String> hashMap = new HashMap<>();

            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    String columnName = rsmd.getColumnName(i);
                    String columnValue = rs.getString(i);

                    hashMap.put(columnName, columnValue);

                    columns.append("column").append(i);
                    values.append("?");

                    if (i <= columnsNumber - 1) {
                        columns.append(", ");
                        values.append(", ");
                    }
                }
            }

            insertSql = "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + values + ")";
            System.out.println(insertSql);

            if (properties.getProperty("b_print_results").equals("true")) {
                dumpMap(hashMap);
            }

            preparedStatement = connection.prepareStatement(insertSql);

            int i = 1;
            for (String value : hashMap.values()) {
                preparedStatement.setString(i, value);
                i++;
            }

            preparedStatement.executeUpdate();
            System.out.println("Record inserted into table '" + properties.getProperty("exportToTableName") + "'");

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
        if (args != null && args.length == 1) {
            configFileName = args[0];
        }
        
        System.out.println("Using the config file: " + configFileName);

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
