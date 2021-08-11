package com.sfz.geekfileformat;

/**
 * @author sfz
 * @date 2021/8/9 下午6:01
 * @version 1.0
 */

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.ql.io.AbstractStorageFormatDescriptor;

import java.util.Set;

/**
 * @Description: GeekFileInputFormat
 * @author: sfz
 * @date: 2021年08月09日 下午6:01
 */
public class GeekFileStorageFormatDescriptor extends AbstractStorageFormatDescriptor {
    @Override
    public Set<String> getNames() {
        return ImmutableSet.of("GEEK","GeekFileFormat");
    }

    @Override
    public String getInputFormat() {
        return GeekTextInputFormat.class.getName();
    }

    @Override
    public String getOutputFormat() {
        return GeekTextOutputFormat.class.getName();
    }

}
