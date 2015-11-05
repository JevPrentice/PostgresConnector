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
public class Singleton {

    private static final Singleton singleton = new Singleton();
    private static Properties properties;

    private Singleton() {
        try {
            Class.forName(properties.getProperty("database_driver"));
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Singleton.class.getName()).log(Level.SEVERE, null, ex);
        }
        Logger.getLogger(Singleton.class.getName()).log(Level.INFO, "JDBC Driver is working.");

    }

    public static Singleton getInstance(String configFile) {
        try {
            properties.load(new FileInputStream(configFile));
            return singleton;
        } catch (IOException e) {
            Logger.getLogger(Singleton.class.getName()).log(Level.SEVERE, "Unable to load properties from: " + System.getProperty("user.dir") + "/" + configFile, e);
            return null;
        }
    }

    /**
     * Remember to Close this Connection once done!
     *
     * @return
     */
    public static Connection getConnection() {
        return Singleton.createConnection();
    }

    private static Connection createConnection() {
        Connection connection = null;
        try {
            String database = properties.getProperty("database_url");
            connection = DriverManager.getConnection(database, properties.getProperty("database_user"), properties.getProperty("database_password"));
            Logger.getLogger(Singleton.class.getName()).log(Level.INFO, "Connection created to {0}", database);
        } catch (SQLException e) {
            Logger.getLogger(Singleton.class.getName()).log(Level.SEVERE, "SQL Exception while trying to create database connection", e);
        }
        return connection;
    }

    private static String getSqlFileText() throws FileNotFoundException {
        String pathname = properties.getProperty("sql_filename");

        File file = new File(pathname);
        if (!file.exists()) {
            FileNotFoundException e = new FileNotFoundException();
            Logger.getLogger(Singleton.class.getName()).log(Level.SEVERE, "The file: " + System.getProperty("user.dir") + "/" + pathname + " does not exist.\nAborting Now.", e);
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

    public void performQuery(Connection connection) throws SQLException, FileNotFoundException {

        DatabaseMetaData metadata = connection.getMetaData();

        String tableName = properties.getProperty("destinationTable");
        String schemaName = properties.getProperty("destinationSchema");

        ResultSet tables = metadata.getTables(null, null, tableName, null);
        if (!tables.next()) {
            Logger.getLogger(Singleton.class.getName()).log(Level.INFO, "The table {0}.{1} does not exist. \nAborting Now.", new Object[]{schemaName, tableName});
            return;
        }

        try (Statement stmt = connection.createStatement()) {
            if (properties.getProperty("b_do_truncate_export_table").equals("true")) {
                String truncateSql = "TRUNCATE " + schemaName + "." + tableName;
                stmt.executeUpdate(truncateSql);
                Logger.getLogger(Singleton.class.getName()).log(Level.INFO, "{0} - successful", truncateSql);
            }

            String selectSql = getSqlFileText();

            if (properties.getProperty("b_print_sql").equals("true")) {
                Logger.getLogger(Singleton.class.getName()).log(Level.INFO, "Now Executing the following SQL:\n***** SQL START *****\n{0}\n***** SQL END *****", selectSql);
            }

            StringBuilder columns;
            StringBuilder values;
            ArrayList<ArrayList> queryResultList;
            try (ResultSet rs = stmt.executeQuery(selectSql)) {

                ResultSetMetaData rsmd = rs.getMetaData();

                int columnsNumber = rsmd.getColumnCount();
                Logger.getLogger(Singleton.class.getName()).log(Level.INFO, "Number Columns returned: {0}", columnsNumber);

                if (columnsNumber <= 0) {
                    Logger.getLogger(Singleton.class.getName()).log(Level.INFO, "There were no columns returned by the query.");
                    return;
                } else if (columnsNumber > 9) {
                    Logger.getLogger(Singleton.class.getName()).log(Level.INFO, "This query returns {0} columns, but the MAX is 9. :(", columnsNumber);
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
                Logger.getLogger(Singleton.class.getName()).log(Level.INFO, "Query Result List: {0}", queryResultList.toString());
            }

            if (queryResultList.size() <= 0) {
                Logger.getLogger(Singleton.class.getName()).log(Level.INFO, "Query Returned No Results");
                return;
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

            Logger.getLogger(Singleton.class.getName()).log(Level.INFO, "{0} - successful {1} record(s) inserted", new Object[]{insertSql, i});
        }
    }
}
