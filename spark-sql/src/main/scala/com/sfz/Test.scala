package com.sfz

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * @author sfz
 * @date 2021/9/6 下午3:31
 * @version 1.0
 */
object Test {

  def main(args: Array[String]): Unit = {

    // $example on:init_session$
    println("开始")
    val conf = new SparkConf().setMaster("local")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL TEST")
      .config(conf)
      .config("spark.some.config.option", "some-value")
      .config("spark.sql.planChangeLog.level", "warn")
      .withExtensions(extensions => {
        extensions.injectOptimizerRule {
          session => {
            MyPushDown(session)
          }
        }
      })
      .getOrCreate()


    val df = spark.read.json("spark-sql/src/main/resources/student.json")
    df.createOrReplaceTempView("people")
    spark.sql("select age,name from (select age,name,sex from student)").show


    println("完成")
    spark.stop()

  }
}

case class MyPushDown(session: SparkSession) extends Rule[LogicalPlan]  {

  def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
    case a: LogicalPlan => println("插入优化逻辑:"+a)
      a
  }


}
