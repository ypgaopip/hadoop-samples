package org.example;

import java.sql.*;

// 测试JDBC连接
public class BasicConnection {
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://10.211.55.4:3306/hadoop";
    static final String USER = "root";
    static final String PASS = "123";


    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName(JDBC_DRIVER);
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            System.out.println("Creating statement...");
            stmt = conn.createStatement();
            String sql;
            sql = "SELECT word, times FROM test";
            ResultSet rs = stmt.executeQuery(sql);
            System.out.println("get rs");
            while (rs.next()) {
                String word = rs.getString("word");
                String times = rs.getString("times");
                System.out.print("Word: " + word);
                System.out.println(", Times: " + times);
            }
            rs.close();
            stmt.close();
            conn.close();
        } catch (SQLException se) {
            se.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se2) {
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
        System.out.println("Goodbye!");
    }
}