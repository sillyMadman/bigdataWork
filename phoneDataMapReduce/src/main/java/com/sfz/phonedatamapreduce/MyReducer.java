package com.sfz.phonedatamapreduce;
/**
 * @author sfz
 * @date 2021/7/15 3:00 下午
 * @version 1.0
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description: reduce实现
 * @author: sfz
 * @date: 2021年07月15日 3:00 下午
 */
public class MyReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        Long sumUpFlow=0L;
        Long sumDownFlow=0L;
        for(FlowBean flowBean:values){
            sumUpFlow+=flowBean.getUpFlow();
            sumDownFlow+=flowBean.getDownFlow();
        }


        FlowBean flow= new FlowBean(sumUpFlow,sumDownFlow);
        context.write(key,flow);
    }



}
