package com.sfz.presto;/**
 * @author sfz
 * @date 2021/9/29 下午5:30
 * @version 1.0
 */

import java.sql.*;
import java.util.Properties;

/**
 * @Description: jdbc to presto
 * @author: sfz
 * @date: 2021年09月29日 下午5:30
 */
public class PrestoJdbcTest {

    public static void main(String[] args) {
         String sql;
        if (args.length != 0) {
            sql= args[0];
        } else {
          sql = "select * from shafengzi_base";
        }


        try {
            Class.forName("com.facebook.presto.jdbc.PrestoDriver");
        } catch (ClassNotFoundException e) {

            System.out.println("Failed to load presto jdbc driver.");
            System.exit(-1);
        }
        Connection connection = null;
        Statement statement = null;
        try {
//            String   url = "jdbc:presto://172.16.63.32:9090/hive/default";
            String url = "jdbc:presto://106.15.194.185:9090/hive/default";
            Properties properties = new Properties();
            properties.setProperty("user", "hadoop");
            // 创建连接对象。
            connection = DriverManager.getConnection(url, properties);
            // 创建Statement对象。
            statement = connection.createStatement();
            // 执行查询。
            ResultSet rs = statement.executeQuery(sql);
            // 获取结果。
            int columnNum = rs.getMetaData().getColumnCount();
            int rowIndex = 0;
            while (rs.next()) {
                rowIndex++;
                for (int i = 1; i <= columnNum; i++) {
                    System.out.println("Row " + rowIndex + ", Column " + i + ": " + rs.getString(i));
                }
            }
        } catch (SQLException e) {

            System.out.println("Exception thrown."+e);
        } finally {
            // 销毁Statement对象。
            if (statement != null) {
                try {
                    statement.close();
                } catch (Throwable t) {
                    System.out.println(t);
                }
            }
            // 关闭连接。
            if (connection != null) {
                try {
                    connection.close();
                } catch (Throwable t) {
                    System.out.println(t);
                }


            }
        }
    }
}