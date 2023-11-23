package org.example.hive.case2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class GetConnect {

    private static Connection conntohive = null;
    private static Connection conntomysql = null;

    private static final String HIVE_DBDRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";
    private static final String HIVE_DBURL = "jdbc:hive2://10.211.55.4:10000/default";
    private static final String HIVE_DBUSERNAME = "parallels";
    private static final String HIVE_DBPASSWORD = "1";

    private static final String MYSQL_DBDRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static final String MYSQL_DBURL = "jdbc:mysql://10.211.55.4:3306/hadooplogs?useUnicode=true&characterEncoding=UTF8";
    private static final String MYSQL_DBUSERNAME = "root";
    private static final String MYSQL_DBPASSWORD = "123";

    private GetConnect() {

    }

    public static Connection getHiveConn() throws SQLException {

        if (conntohive == null) {
            try {
                Class.forName(HIVE_DBDRIVER_CLASS);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }

            conntohive = DriverManager.getConnection(
                    HIVE_DBURL, HIVE_DBUSERNAME, HIVE_DBPASSWORD);
            System.out.println("Connect Hive Success!");
        }
        return conntohive;

    }

    public static Connection getMysqlConn() throws SQLException {
        if (conntomysql == null) {
            try {
                Class.forName(MYSQL_DBDRIVER_CLASS);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }

            conntomysql = DriverManager
                    .getConnection(
                            MYSQL_DBURL, MYSQL_DBUSERNAME, MYSQL_DBPASSWORD);
            System.out.println("Connect Mysql Success!");
        }
        return conntomysql;
    }

    public static void closeHive() throws SQLException {
        if (conntohive != null)
            conntohive.close();
    }

    public static void closemysql() throws SQLException {
        if (conntomysql != null)
            conntomysql.close();
    }
}

