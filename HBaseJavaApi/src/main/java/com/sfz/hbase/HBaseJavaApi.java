package com.sfz.hbase;/**
 * @author sfz
 * @date 2021/7/28 7:20 下午
 * @version 1.0
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * @Description: TODO
 * @author: scott
 * @date: 2021年07月28日 7:20 下午
 */
public class HBaseJavaApi {

    private static Logger logger = LoggerFactory.getLogger(HBaseJavaApi.class);

    public static void main(String[] args) {
        String tableName = "lihaowu:student";
        HBaseUtil hBaseUtil = new HBaseUtil();

        try {
            //获取连接
            logger.info("获取连接");
            Connection connection = hBaseUtil.initHbase();
            Admin admin = connection.getAdmin();

            //创建表
//            hBaseUtil.createTable(admin,tableName, new String[]{"info", "score"});

            //查询数据
            logger.info("获取全表数据");
            hBaseUtil.getScanner(connection,tableName);



            //添加数据初始化
            Student studentTom = new Student("Tom","20210000000001","1","75","82");
            Student studentJerry = new Student("Jerry","20210000000002","1","85","67");
            Student studentJack = new Student("Jack","20210000000003","1","80","80");
            Student studentRose = new Student("Rose","20210000000004","1","60","61");
            Student studentLihaowu = new Student("lihaowu","G20210735010280","3","85","85");


            //添加数据
            logger.info("添加数据");
//            hBaseUtil.put(connection,tableName,studentTom);
//            hBaseUtil.put(connection,tableName,studentJerry);
//            hBaseUtil.put(connection,tableName,studentJack);
//            hBaseUtil.put(connection,tableName,studentRose);
//            hBaseUtil.put(connection,tableName,studentLihaowu);
            logger.info("按行键查询");
            //按行键查询数据
            hBaseUtil.get(connection,tableName,"Tom");
            hBaseUtil.get(connection,tableName,"Jerry");
            hBaseUtil.get(connection,tableName,"Jack");
            hBaseUtil.get(connection,tableName,"Rose");
            hBaseUtil.get(connection,tableName,"lihaowu");


//
//
//            //删除数据
              logger.info("按行键删除数据");
//            hBaseUtil.delete(connection,tableName,"Tom");
//            hBaseUtil.delete(connection,tableName,"Jerry");
//            hBaseUtil.delete(connection,tableName,"Jack");
//            hBaseUtil.delete(connection,tableName,"Rose");
//            hBaseUtil.delete(connection,tableName,"lihaowu");



            admin.close();
            connection.close();
            logger.info("断开连接");


        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}
