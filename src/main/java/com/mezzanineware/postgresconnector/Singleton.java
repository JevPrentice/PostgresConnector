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
        System.out.println("JDBC Driver working.");
    }

    public static Singleton getInstance(String configFile) throws IOException, SQLException {
        properties.load(new FileInputStream(configFile));
        return singleton;
    }

    public static Connection getConnection() {
        return Singleton.createConnection();
    }

    private static Connection createConnection() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(properties.getProperty("database_url"), properties.getProperty("database_user"), properties.getProperty("database_password"));
        } catch (SQLException ex) {
            Logger.getLogger(Singleton.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("Connection created.");
        return connection;
    }

    private static String getSqlFileText() throws IOException {
        String pathname = properties.getProperty("sql_filename");

        File file = new File(pathname);
        if (!file.exists()) {
            System.out.println("The file: " + System.getProperty("user.dir") + "/" + pathname + " does not exist.\nAborting Now.");
        }

        StringBuilder fileContents = new StringBuilder((int) file.length());
        try (Scanner scanner = new Scanner(file)) {
            while (scanner.hasNextLine()) {
                fileContents.append(scanner.nextLine()).append(System.getProperty("line.separator"));
            }
            return fileContents.toString();
        }
    }

    public void performQuery(Connection connection) throws SQLException, IOException {

        DatabaseMetaData metadata = connection.getMetaData();

        String tableName = properties.getProperty("destinationTable");
        String schemaName = properties.getProperty("destinationSchema");

        ResultSet tables = metadata.getTables(null, null, tableName, null);
        if (!tables.next()) {
            System.out.println("The table " + schemaName + "." + tableName + " does not exist. \nAborting Now.");
            return;
        }

        try (Statement stmt = connection.createStatement()) {
            if (properties.getProperty("b_do_truncate_export_table").equals("true")) {
                String truncateSql = "TRUNCATE " + schemaName + "." + tableName;
                stmt.executeUpdate(truncateSql);
                System.out.println(truncateSql + " - successful");
            }

            String selectSql = getSqlFileText();

            if (properties.getProperty("b_print_sql").equals("true")) {
                System.out.println("Now Executing the following SQL:\n***** SQL START *****\n" + selectSql + "\n***** SQL END *****");
            }

            StringBuilder columns;
            StringBuilder values;
            ArrayList<ArrayList> queryResultList;
            try (ResultSet rs = stmt.executeQuery(selectSql)) {
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnsNumber = rsmd.getColumnCount();
                System.out.println("Number Columns returned: " + columnsNumber);
                if (columnsNumber <= 0) {
                    System.out.println("There were no columns returned by the query.");
                    return;
                } else if (columnsNumber > 9) {
                    System.out.println("This query returns " + columnsNumber + " columns, but the MAX is 9. :(");
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
                System.out.println("Query Result List: " + queryResultList.toString());
            }

            if (queryResultList.size() <= 0) {
                System.out.println("Query Returned No Results");
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

            System.out.println(insertSql + " - successful " + i + " record(s) inserted");
        }
    }
}