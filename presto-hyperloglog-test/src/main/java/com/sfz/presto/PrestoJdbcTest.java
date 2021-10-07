package com.sfz.presto;/**
 * @author sfz
 * @date 2021/9/29 下午5:30
 * @version 1.0
 */

import java.sql.*;

/**
 * @Description: jdbc to presto
 * @author: sfz
 * @date: 2021年09月29日 下午5:30
 */
public class PrestoJdbcTest {

    public static void main(String[] args) throws  SQLException {

        try {
            Class.forName("com.facebook.presto.jdbc.PrestoDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        String url = "jdbc:presto://106.15.194.185:9090/hive/default?user=student03&SSL=true";
        Connection connection = DriverManager.getConnection(url);
        Statement stmt = connection.createStatement();

//        ResultSet rs = stmt.executeQuery("show tables");
        ResultSet rs = stmt.executeQuery("select * from shafengzi_base");

        while (rs.next()) {
            System.out.println("rs.getString(1) = " + rs.getString(1));

        }

        rs.close();

    }
}
