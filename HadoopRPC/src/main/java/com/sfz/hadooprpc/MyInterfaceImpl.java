package com.sfz.hadooprpc;/**
 * @author sfz
 * @date 2021/7/20 6:58 下午
 * @version 1.0
 */

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

/**
 * @Description: 实现RPC接口
 * @author: sfz
 * @date: 2021年07月20日 6:58 下午
 */
public class MyInterfaceImpl implements MyInterface {
    String returnNull = "20210000000000";
    String returnXinXin = "20210123456789";
    String returnMyName = "G20210735010280";
    public String findName(String studentId) {
        if (returnNull.equals(studentId)) {
            System.out.println("返回:null");
            return null;
        } else if (returnXinXin.equals(studentId)) {
            System.out.println("返回:心心");
            return "心心";
        } else if (returnMyName.equals(studentId)) {
            System.out.println("返回:傻疯子");
            return "傻疯子";
        }

        return null;
    }

    public long getProtocolVersion(String s, long l) throws IOException {
        return MyInterface.versionID;
    }

    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return new ProtocolSignature();
    }
}
