package com.sfz.geekfileformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author sfz
 * @version 1.0
 * @date 2021/8/10 下午3:18
 */
public class GeekFileInputFormatTest {


    @Test
    public void nextTest() {
        Text value = new Text("This notebook can be geeeek used to geek install gek on all geeeek worker nodes, run data generation, and create the TPCDS geeeeeeeeek database.");

        String strReplace = value.toString().replaceAll(" ge{2,256}k", "");
        Text txtReplace = new Text();
        txtReplace.set(strReplace);
        value.set(txtReplace);

        assertEquals(value,new Text("This notebook can be used to install gek on all worker nodes, run data generation, and create the TPCDS database."));



    }
}