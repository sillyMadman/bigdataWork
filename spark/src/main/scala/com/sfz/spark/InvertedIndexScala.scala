package com.sfz.spark

import org.apache.spark.{SparkConf, SparkContext}


/**
 * @author sfz
 * @date 2021/8/19 上午9:52
 * @version 1.0
 */
object InvertedIndexScala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("InvertedIndexScala")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val path0 = "spark/src/main/resources/data/0.txt"
    val path1 = "spark/src/main/resources/data/1.txt"
    val path2 = "spark/src/main/resources/data/2.txt"

    //取出文件数据并添加上对应的文件名
    val linesRDD0 = sc.textFile(path0)
    val linesRDD1 = sc.textFile(path1)
    val linesRDD2 = sc.textFile(path2)
    val wordsRDD0 = linesRDD0.flatMap(_.split(" "))
    //[it,is,what,it,is]
    val wordsRDD1 = linesRDD1.flatMap(_.split(" "))
    val wordsRDD2 = linesRDD2.flatMap(_.split(" "))
    val pairRDD0 = wordsRDD0.map((_, 0))
    //[(it,0),(is,0),(what,0),(it,0),(is,0)]
    val pairRDD1 = wordsRDD1.map((_, 1))
    val pairRDD2 = wordsRDD2.map((_, 2))

    //拿到所有数据
    val pairRDD = pairRDD0.union(pairRDD1).union(pairRDD2).persist()
    //获取反向文件索引
    val indexRdd = pairRDD.distinct().groupByKey().sortByKey().map(t=>(t._1,t._2.toList.sorted))
    println("反向文件索引==========================================")
    indexRdd.foreach(indexRdd => println(indexRdd._1.mkString("\"", "", "\"") + ":  " + indexRdd._2.mkString("{", ",", "}")))
    //获取反向文件索引和词频=
    val x = pairRDD.map(x => ((x._1, x._2),1)).reduceByKey(_ + _)
    println("获取反向文件索引和词频==========================================")
    x.map(x=>(x._1._1,(x._1._2,x._2))).groupByKey().sortByKey().map(t=>(t._1,t._2.toList.sortBy(_._1))).foreach(x=>println(x._1.mkString("\"", "", "\"")+":  "+x._2.mkString("{", ",", "}")))


    sc.stop()


  }

}
