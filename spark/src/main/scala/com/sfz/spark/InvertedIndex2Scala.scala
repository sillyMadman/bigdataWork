package com.sfz.spark

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import scala.collection.mutable.ArrayBuffer

//优化获取文件的方式
object InvertedIndex2Scala {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("InvertedIndex2Scala")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val input = "spark/src/main/resources/data/"
//    val fs = FileSystem.get(sc.hadoopConfiguration)
    val file = new File(input)
    val fileList = file.listFiles().iterator
    var rdd = sc.emptyRDD[(String, String)]
    while (  fileList.hasNext) {
      val path = fileList.next()
      val fileName = path.getName
      rdd = rdd.union(sc.textFile(path.getPath.toString).flatMap(_.split("\\s+"))
        .map((_,fileName)))
    }

    println("--"*100)
    rdd.foreach(println)
    println("--"*100)

    val indexRdd = rdd.distinct().groupByKey().sortByKey().map(t=>(t._1,t._2.toList.sorted))
    println("--"*100)
    indexRdd.foreach(indexRdd => println(indexRdd._1.mkString("\"", "", "\"") + ":  " + indexRdd._2.mkString("{", ",", "}")))
    println("--"*100)

    val x = rdd.map(x => ((x._1, x._2),1)).reduceByKey(_ + _)
    println("获取反向文件索引和词频==========================================")
    x.map(x=>(x._1._1,(x._1._2,x._2))).groupByKey().sortByKey().map(t=>(t._1,t._2.toList.sortBy(_._1))).foreach(x=>println(x._1.mkString("\"", "", "\"")+":  "+x._2.mkString("{", ",", "}")))





  }

}

