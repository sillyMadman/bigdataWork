package com.sfz.geekfileformat;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;

/**
 * @author sfz
 * @version 1.0
 * @date 2021/8/11 上午10:56
 */
public class GeekTextOutputFormatTest {

//  GeekTextOutputFormat.GeekRecordWriter geekRecordWriter;


    @Test
    public void writeTest()  {


        Text text = new Text("This notebook can be used to install gek on all worker nodes, run data generation, and create the TPCDS database.");
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
        System.out.println(newStr.toString());
    }

}