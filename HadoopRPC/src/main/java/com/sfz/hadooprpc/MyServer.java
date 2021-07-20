package com.sfz.hadooprpc;/**
 * @author sfz
 * @date 2021/7/20 7:06 下午
 * @version 1.0
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * @Description: RPC服务器
 * @author: sfz
 * @date: 2021年07月20日 7:06 下午
 */
public class MyServer {
    public static void main(String[] args) {
//
        //创建RPC Server的构建器
        RPC.Builder builder = new RPC.Builder(new Configuration());
        //设置构建器相关的参数
        /**
         * 设置监听的IP或者主机名
         * 设置监听的端口
         * 设置RPC接口
         * 设置RPC接口的实现类
         */
        builder.setBindAddress("127.0.0.1")
                .setPort(12345)
                .setProtocol(MyInterface.class)
                .setInstance(new MyInterfaceImpl());

        //构建 RPC Server
        RPC.Server server = null;
        try {
            server = builder.build();

        //启动
        server.start();
        System.out.println("RPC Server(服务器)启动了...");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
