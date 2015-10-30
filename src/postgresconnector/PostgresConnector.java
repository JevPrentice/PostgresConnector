package postgresconnector;

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

    private void registerPostgresJDBC() throws ClassNotFoundException {
        Class.forName(properties.getProperty("database_driver"));
    }

    private Connection getPostgresJDBCConnection() throws SQLException {
        String url = properties.getProperty("database_url");
        Properties props = new Properties();
        props.setProperty("user", properties.getProperty("database_user"));
        props.setProperty("password", properties.getProperty("database_password"));
        props.setProperty("ssl", "true");
        return DriverManager.getConnection(url, props);
    }

    private static String readFile(String pathname) throws IOException {

        File file = new File(pathname);
        StringBuilder fileContents = new StringBuilder((int) file.length());
        Scanner scanner = new Scanner(file);
        String lineSeparator = System.getProperty("line.separator");

        try {
            while (scanner.hasNextLine()) {
                fileContents.append(scanner.nextLine() + lineSeparator);
            }
            return fileContents.toString();
        } finally {
            scanner.close();
        }
    }

    private void performQuery(Connection connection) throws SQLException {

        ResultSet rs = null;
        Statement stmt = null;
        PreparedStatement preparedStatement;

        try {

            stmt = connection.createStatement();

            String sql = readFile(properties.getProperty("sql_filename"));

            if (properties.getProperty("b_print_sql").equals("true")) {
                System.out.println("Now Executing the following SQL:\n***** SQL START *****\n" + sql + "\n***** SQL END *****");

            }

            rs = stmt.executeQuery(sql);

            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            String insertSql = "INSERT INTO jev_test (column1, column2, column3, column4, column5, column6) VALUES (";
            HashMap<String, String> hashMap = new HashMap<>();
            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    String columnName = rsmd.getColumnName(i);
                    String columnValue = rs.getString(i);
                    hashMap.put(columnName, columnValue);
                }
            }

            if (hashMap.size() <= 0) {
                System.out.println("There were no results returned by the query.");
                return;
            }

            for (int i = 1; i <= hashMap.size(); i++) {
                insertSql = insertSql + "?";
                if (i <= hashMap.size() - 1) {
                    insertSql = insertSql + ", ";
                }
            }

            insertSql = insertSql + ")";
            System.out.println(insertSql);

            preparedStatement = connection.prepareStatement(insertSql);

            int i = 1;
            for (Map.Entry<String, String> entry : hashMap.entrySet()) {

                String key = entry.getKey();
                String value = entry.getValue();

                preparedStatement.setString(i, value);

//                System.out.println(i + " - " + value);
                i++;
            }

            preparedStatement.executeUpdate();
            System.out.println("Record is inserted into table!");

        } catch (Exception e) {
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

        PostgresConnector pgConn = new PostgresConnector("config.properties");
        Connection connection = null;

        try {
            pgConn.registerPostgresJDBC();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }

        try {
            connection = pgConn.getPostgresJDBCConnection();
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
    }

}
