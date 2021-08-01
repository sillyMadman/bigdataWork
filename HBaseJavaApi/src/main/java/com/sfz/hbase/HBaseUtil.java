package com.sfz.hbase;/**
 * @author sfz
 * @date 2021/7/29 3:59 下午
 * @version 1.0
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * @Description: TODO
 * @author: scott
 * @date: 2021年07月29日 3:59 下午
 */
public class HBaseUtil {
    private Admin admin;
    private static Logger logger = LoggerFactory.getLogger(HBaseUtil.class);


    public Connection initHbase() throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "47.101.206.249");
        configuration.set("hbase.master", "47.101.216.12:16000");
        //Connection instance
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

    public void createTable(String tableName, String[] cols) throws Exception {
        admin = initHbase().getAdmin();

        //命名空间如果无，创建命名空间
        if (tableName.contains(":")) {
            admin.createNamespace(NamespaceDescriptor.create(tableName.split(":")[0]).build());
        }


        //如果表不存在，创建表
        if (admin.tableExists(TableName.valueOf(tableName))) {
            logger.warn("表已存在！");
        } else {

            ArrayList<ColumnFamilyDescriptor> familyList = new ArrayList<ColumnFamilyDescriptor>();
            for (String col : cols) {
                ColumnFamilyDescriptor familyDesc = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(col))
                        .setMaxVersions(3)
                        .build();
                familyList.add(familyDesc);

            }
            TableDescriptor desc = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                    .setColumnFamilies(familyList)
                    .build();
            admin.createTable(desc);
        }


    }

    public ResultScanner getScanner(String tableName) throws Exception {
        admin = initHbase().getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            Table table = initHbase().getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setCaching(1000);
            ResultScanner results = table.getScanner(scan);
            results.forEach(result -> {
                System.out.println("rowkey == " + Bytes.toString(result.getRow()));
                System.out.println("basic:name == " + Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("student_id"))));
                System.out.println("basic:name == " + Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("class"))));
                System.out.println("basic:name == " + Bytes.toString(result.getValue(Bytes.toBytes("score"), Bytes.toBytes("understanding"))));
                System.out.println("basic:name == " + Bytes.toString(result.getValue(Bytes.toBytes("score"), Bytes.toBytes("programming"))));
            });
            return results;
        } else {
            logger.warn("表不存在");
            return null;
        }

    }


}
