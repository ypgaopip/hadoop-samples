package org.example.hive.case2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveUtil {

    public static void createTable(String hiveql) throws SQLException {
        Connection con = GetConnect.getHiveConn();
        Statement stmt = con.createStatement();
        stmt.execute(hiveql);
    }

    public static ResultSet queryHive(String hiveql) throws SQLException {
        Connection con = GetConnect.getHiveConn();
        Statement stmt = con.createStatement();
        ResultSet res = stmt.executeQuery(hiveql);
        return res;
    }

    public static void loadData(String hiveql) throws SQLException {
        Connection con = GetConnect.getHiveConn();
        Statement stmt = con.createStatement();
        stmt.executeUpdate(hiveql);
    }

    public static void hiveTomysql(ResultSet Hiveres) throws SQLException {
        Connection con = GetConnect.getMysqlConn();
        Statement stmt = con.createStatement();
        while (Hiveres.next()) {
            String rdate = Hiveres.getString(1);
            String time = Hiveres.getString(2);
            String type = Hiveres.getString(3);
            String relateclass = Hiveres.getString(4);
            String information = Hiveres.getString(5) + Hiveres.getString(6)
                    + Hiveres.getString(7);
            System.out.println(rdate + " " + time + " " + type + " "
                    + relateclass + " " + information + " ");
            int i = stmt.executeUpdate("insert into hadoop_loginfo values(0,'"
                    + rdate + "','" + time + "','" + type + "','" + relateclass
                    + "','" + information + "')");
        }
    }

}

