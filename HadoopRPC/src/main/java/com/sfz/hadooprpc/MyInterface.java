package com.sfz.hadooprpc;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * @author sfz
 * @version 1.0
 * @date 2021/7/20 6:54 下午
 */
public interface MyInterface extends VersionedProtocol {
    long versionID = 1L;

    /**
     * @param studentId 学号
     * @return 返回姓名
     */
    String findName(String studentId);

}
