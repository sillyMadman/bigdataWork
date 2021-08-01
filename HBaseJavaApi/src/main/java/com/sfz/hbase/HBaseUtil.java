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
import java.util.List;

/**
 * @Description: TODO
 * @author: scott
 * @date: 2021年07月29日 3:59 下午
 */
public class HBaseUtil {

    private static Logger logger = LoggerFactory.getLogger(HBaseUtil.class);

    /**
     * 获取Hbase的连接
     *
     * @return Connection Hbase的连接
     * @throws Exception
     */
    public Connection initHbase() throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "47.101.206.249");
        configuration.set("hbase.master", "47.101.216.12:16000");
        //Connection instance
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

    /**
     * @param admin  获取管理权限
     * @param tableName 命名空间:表名。
     * @param cols      列簇
     * @throws Exception
     */
    public void createTable(Admin admin,String tableName, String[] cols) throws Exception {


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

    /**
     * 查询表数据
     * @param connection
     * @param tableName
     * @return
     * @throws Exception
     */
    public ResultScanner getScanner(Connection connection,String tableName) throws Exception {
        if (connection.getAdmin().tableExists(TableName.valueOf(tableName))) {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setCaching(1000);
            ResultScanner results = table.getScanner(scan);
            results.forEach(result -> {
                System.out.println("rowkey == " + Bytes.toString(result.getRow()));
                System.out.println("info:class == " + Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("student_id"))));
                System.out.println("info:name == " + Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("class"))));
                System.out.println("score:understanding == " + Bytes.toString(result.getValue(Bytes.toBytes("score"), Bytes.toBytes("understanding"))));
                System.out.println("score:programming == " + Bytes.toString(result.getValue(Bytes.toBytes("score"), Bytes.toBytes("programming"))));
            });
            table.close();
            return results;

        } else {
            logger.warn("表不存在");
            return null;
        }

    }

    /**
     * 添加数据
     * @param connection hbase连接
     * @param tableName 表名
     * @param rowKey  行键
     * @param student_id 学号
     * @param classNumber 班级
     * @param understanding 理解得分
     * @param programming 编程得分
     * @throws Exception
     */
    public void put(Connection connection,String tableName,String rowKey,String student_id,String classNumber,String understanding,String programming) throws Exception {

        Table table = connection.getTable(TableName.valueOf(tableName));

        Put put =new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("student_id"),Bytes.toBytes(student_id));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("class"),Bytes.toBytes(classNumber));
        put.addColumn(Bytes.toBytes("score"),Bytes.toBytes("understanding"),Bytes.toBytes(classNumber));
        put.addColumn(Bytes.toBytes("score"),Bytes.toBytes("programming"),Bytes.toBytes(classNumber));
        table.put(put);
        table.close();


    }

    /**
     * 添加数据
     * @param connection hbase连接
     * @param tableName 表明
     * @param student 学生实体类
     * @throws Exception
     */

    public void put(Connection connection,String tableName,Student student) throws Exception {

        Table table = connection.getTable(TableName.valueOf(tableName));

        Put put =new Put(Bytes.toBytes(student.getName()));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("student_id"),Bytes.toBytes(student.getStudentId()));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("class"),Bytes.toBytes(student.getClassNumber()));
        put.addColumn(Bytes.toBytes("score"),Bytes.toBytes("understanding"),Bytes.toBytes(student.getUnderstanding()));
        put.addColumn(Bytes.toBytes("score"),Bytes.toBytes("programming"),Bytes.toBytes(student.getProgramming()));
        table.put(put);
        table.close();


    }

    /**
     * 删除数据
     * @param connection hbase连接
     * @param tableName 表名
     * @param rowKey 行键
     * @throws Exception
     */
    public void delete(Connection connection,String tableName,String rowKey) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Delete delete   = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        table.close();

    }

    /**
     * 查询数据
     * @param connection hbase连接
     * @param tableName 表名
     * @param rowKey 行键
     * @throws Exception
     */
    public void get(Connection connection,String tableName,String rowKey) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        List<Cell> cells =result.listCells();
        for(Cell cell: cells){
            byte[] family = CellUtil.cloneFamily(cell);
            byte[] column = CellUtil.cloneQualifier(cell);
            byte[] value = CellUtil.cloneValue(cell);
            System.out.println(rowKey+":"+new String(family)+":"+new String(column) +"="+new String(value));

        }
        table.close();
    }

    public void deleteTable(Admin admin,String tableName) throws Exception{
        admin.deleteTable(TableName.valueOf(tableName));
    }


}
