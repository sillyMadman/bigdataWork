package com.sfz.phonedatamapreduce;
/**
 * @author sfz
 * @date 2021/7/15 3:38 下午
 * @version 1.0
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Description: MAPREDUCE_JOB组装入口
 * @author: sfz
 * @date: 2021年07月15日 3:38 下午
 */
public class FlowCountJob {

        /**
         * 组装job=map+reduce
         * @param args
         */
        public static void main(String[] args) {
            try {
                if(args.length!=2){
                    //  如果传递的参数不够，程序直接退出
                    System.exit(100);
                }
                //  job需要的配置参数
                Configuration conf = new Configuration();
                //  创建一个job
                Job job = Job.getInstance(conf);


                job.setJarByClass(FlowCountJob.class);

                //  指定输入路径(可以是文件，也可以是目录)
                FileInputFormat.setInputPaths(job,new Path(args[0]));
                //  指定输出路径(只能指定一个不存在的目录)
                FileOutputFormat.setOutputPath(job,new Path(args[1]));

                //  指定map相关的代码
                job.setMapperClass(MyMapper.class);
                //  指定k2的类型
                job.setMapOutputKeyClass(Text.class);
                //  指定v2的类型
                job.setMapOutputValueClass(FlowBean.class);

                //  指定reduce相关的代码
                job.setReducerClass(MyReducer.class);
                //  指定k3的类型
                job.setOutputKeyClass(Text.class);
                //  指定v3的类型
                job.setOutputValueClass(FlowBean.class);


                //  提交job
                job.waitForCompletion(true);


            }catch (Exception e){
                e.printStackTrace();
            }

        }


}
