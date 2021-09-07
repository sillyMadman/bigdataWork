package com.sfz.optimize

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * @author sfz
 * @date 2021/9/6 下午3:38
 * @version 1.0
 */
case class MyPushDown(spark: SparkSession) extends Rule[LogicalPlan] {

    def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
      case a: LogicalPlan => println("插入优化逻辑:"+a)
        a
    }

}
