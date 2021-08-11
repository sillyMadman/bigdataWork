package com.sfz.geekfileformat;/**
 * @author sfz
 * @date 2021/8/10 下午2:09
 * @version 1.0
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Properties;

/**
 * @Description: GeekFileOutputFormat
 * @author: sfz
 * @date: 2021年08月10日 下午2:09
 */
public class GeekTextOutputFormat<K extends WritableComparable, V extends Writable>
        extends HiveIgnoreKeyTextOutputFormat<K, V> {

    public static class GeekRecordWriter implements FileSinkOperator.RecordWriter {

        FileSinkOperator.RecordWriter writer;
        Text text;

        public GeekRecordWriter(FileSinkOperator.RecordWriter writer) {
            this.writer = writer;
            this.text = new Text();
        }

        @Override
        public void write(Writable w) throws IOException {
            Text text = new Text((Text) w);
            String str = String.valueOf(text);
            //以空格进行分割
            String[] words = str.split("\\s");
            int min = 2;
            int max = 5;
            //产生2-256的随机数
            int randomNum = (int) (min + Math.random() * (max - min));
            //用于统计ge...k产生之前的单词数
            int wordCount = 0;
            StringBuilder newStr = new StringBuilder();
            for (String word : words) {
                newStr.append(word);
                newStr.append(" ");
                wordCount++;
                if (wordCount < randomNum) {
                    continue;
                }
                String geek = "ge";
                for (int i = 1; i < randomNum; i++) {
                    geek += "e";
                }
                geek += "k";


                newStr.append(geek);
                newStr.append(" ");
                randomNum = (int) (min + Math.random() * (max - min));

                wordCount = 0;


            }
            writer.write(new Text(newStr.toString()));

        }

        @Override
        public void close(boolean abort) throws IOException {
            writer.close(abort);
        }


    }

    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc, Path outPath,
                                                             Class<? extends Writable> valueClass, boolean isCompressed, Properties tableProperties, Progressable progress) throws IOException {

        return new GeekRecordWriter(super.getHiveRecordWriter(jc, outPath, valueClass, isCompressed, tableProperties, progress));
    }
}
