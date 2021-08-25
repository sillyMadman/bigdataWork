package com.sfz.spark.distcp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author sfz
 * @date 2021/8/25 上午11:09
 * @version 1.0
 */
object SparkDistCP {
  def main(args: Array[String]): Unit = {


    val source = "/tmp/test_lhw"
    val target = "/tmp/test_lhw2"

    val configuration = new Configuration()
    configuration.set("fs.defaultFS", "hdfs://sdc01:8020")
    val fileSystem = FileSystem.get(configuration)

    //获取文件目录及文件夹
    val statuses = fileSystem.listStatus(new Path(source))
    for (fileStatus <- statuses) {
      // 如果是文件
      if (fileStatus.isFile()) {
        System.out.println("f:" + fileStatus.getPath().getName());
      } else {
        System.out.println("d:" + fileStatus.getPath().getName());
      }
    }
    val conf = new SparkConf();
    conf.setAppName("SparkDistCP")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val fileRdd = sc.makeRDD(statuses)


    fileRdd.foreach(x => {
      println(x.getPath)
      if(x.isDirectory()){
        println(x.getPath)
        val configuration = new Configuration()
        configuration.set("fs.defaultFS", "hdfs://sdc01:8020")
        val fileSystem = FileSystem.get(configuration)
        fileSystem.mkdirs(new Path("/tmp/test_lhw2/"+x.getPath.getName))

      }
//
    })


  }


}
