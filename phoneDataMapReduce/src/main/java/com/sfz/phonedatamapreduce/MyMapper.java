package com.sfz.phonedatamapreduce;
/**
 * @author sfz
 * @date 2021/7/14 6:17 下午
 * @version 1.0
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description: mapper实现
 * @author: sfz
 * @date: 2021年07月14日 6:17 下午
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] phonedatas = value.toString().split("\t");
        String phoneNum = phonedatas[1];

        long upFlow = Long.valueOf(phonedatas[8]);
        long downFlow = Long.valueOf(phonedatas[9]);

        Text phone = new Text();
        phone.set(phoneNum);

        context.write(phone,new FlowBean(upFlow,downFlow));
    }
}
