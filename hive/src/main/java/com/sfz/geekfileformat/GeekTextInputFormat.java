package com.sfz.geekfileformat;/**
 * @author sfz
 * @date 2021/8/10 上午11:32
 * @version 1.0
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * @Description: FileInputFormat
 * @author: sfz
 * @date: 2021年08月10日 上午11:32
 */
public class GeekTextInputFormat implements
        InputFormat<LongWritable, Text>, JobConfigurable {



    public static class GeekLineRecordReader implements
            RecordReader<LongWritable, Text>, JobConfigurable {

        LineRecordReader reader;
        Text text;



        public GeekLineRecordReader(LineRecordReader reader) {
            this.reader = reader;
            text = reader.createValue();
        }

        @Override
        public boolean next(LongWritable key, Text value) throws IOException {
            while (reader.next(key, text)) {
                //替换2-256个带e的geek
                String strReplace = value.toString().replaceAll(" ge{2,256}k", "");
                Text txtReplace = new Text();
                txtReplace.set(strReplace);
                value.set(txtReplace);

                return true;
            }

            return false;
        }

        @Override
        public LongWritable createKey() {
            return reader.createKey();
        }

        @Override
        public Text createValue() {
            return new Text();
        }

        @Override
        public long getPos() throws IOException {
            return reader.getPos();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        @Override
        public float getProgress() throws IOException {
            return reader.getProgress();
        }


        @Override
        public void configure(JobConf job) {

        }


    }

    TextInputFormat format;
    JobConf job;

    public GeekTextInputFormat() {
        format = new TextInputFormat();

    }


    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        return format.getSplits(job, numSplits);
    }

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(genericSplit.toString());
        GeekTextInputFormat.GeekLineRecordReader reader = new GeekTextInputFormat.GeekLineRecordReader(
                new LineRecordReader(job, (FileSplit) genericSplit));
        reader.configure(job);
        return reader;
    }
    @Override
    public void configure(JobConf job) {
        this.job = job;
        format.configure(job);
    }

}
