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

            hBaseUtil.createTable("lihaowu:student", new String[]{"info", "score"});
            hBaseUtil.getScanner(tableName);
//            hBaseUtil.createTable(");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}
