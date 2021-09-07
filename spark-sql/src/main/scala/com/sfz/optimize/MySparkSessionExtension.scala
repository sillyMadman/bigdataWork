package com.sfz.optimize

import org.apache.spark.sql.SparkSessionExtensions


/**
 * @author sfz
 * @date 2021/9/6 上午11:44
 * @version 1.0
 */


class MySparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule { session =>
       MyPushDown(session)
    }
  }
}
