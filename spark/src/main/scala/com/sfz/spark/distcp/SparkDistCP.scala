package com.sfz.spark.distcp

import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * @author sfz
 * @date 2021/8/25 上午11:09
 * @version 1.0
 */
object SparkDistCP {
  def main(args: Array[String]): Unit = {

    val options = new Options()
    options.addOption("i", "ignore failure ", false, "ignore failures")
    options.addOption("m", "max concurrence ", true, "max concurrence")
    val parser = new DefaultParser()
    val cmd = parser.parse(options, args)

    if (args.length < 2) {
      println("SparkDistCP -m 10 -i input output ")
      System.exit(-1)
    }

    val input = args(args.length - 2)
    val output = args(args.length - 1)

    val IGNORE_FAILURE = cmd.hasOption("i")
    val MAX_CONNCURRENCE = if (cmd.hasOption("m")) cmd.getOptionValue("m").toInt
    else 2


    val conf = new SparkConf()
    conf.setAppName("SparkDistCP")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

//    val configuration = new Configuration()
//    configuration.set("fs.defaultFS", "hdfs://sdc01:8020")
//    val  fileSystem= FileSystem.get(configuration)



    val fileList = fileSystem.listFiles(new Path(input), true)

    val arrayBuffer = ArrayBuffer[String]()


    while (fileList.hasNext) {
      val path = fileList.next().getPath.toString
      arrayBuffer.append(path)
      println(path)
    }

    val rdd = sc.parallelize(arrayBuffer, MAX_CONNCURRENCE)
    rdd.foreachPartition(it => {
      val conf = new Configuration()
      val fs = FileSystem.get(conf)

      while (it.hasNext) {
        val src = it.next()
        val tgt = src.replace(input, output)
        try {
          FileUtil.copy(fs, new Path(src), fs, new Path(tgt), false, conf)
        } catch {
          case ex: Exception =>
            if (IGNORE_FAILURE) println("ignore failure when copy")
            else throw ex
        }
      }


    })




  }


}
