package com.sfz.hadooprpc;/**
 * @author sfz
 * @date 2021/7/20 7:11 下午
 * @version 1.0
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @Description: rpc客户端
 * @author: sfz
 * @date: 2021年07月20日 7:11 下午
 */
public class MyClient {

    public static void main(String[] args) {
        //通过Socket连接RPC Server
        InetSocketAddress addr = new InetSocketAddress("localhost", 12345);
        Configuration conf = new Configuration();

        try {
            MyInterface proxy = RPC.getProxy(MyInterface.class, MyInterface.versionID,addr, conf);
            //执行RPC接口中的方法并获取结果
            String result0 = proxy.findName("G20210735010280");
            String result1 = proxy.findName("20210000000000");
            String result2 = proxy.findName("20210123456789");

            System.out.println("学号G20210735010280姓名为:" + result0);
            System.out.println("学号20210000000000姓名为:" + result1);
            System.out.println("学号20210123456789姓名为:" + result2);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }



}
