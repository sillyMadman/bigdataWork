package com.sfz.spark.distcp

import org.apache.hadoop.conf.{Configuration}


/**
 * @author sfz
 * @date 2021/8/25 下午3:56
 * @version 1.0
 */
class ConfigurationSer(var conf: Configuration) extends Serializable {
  def this() {
    this(new Configuration())
  }

  private def get(): Configuration = conf

  private def writeObject(out: java.io.ObjectOutputStream): Unit = {
    conf.write(out)
  }

  private def readObject(in: java.io.ObjectInputStream): Unit = {
    conf = new Configuration()
    conf.readFields(in)
  }

  private def readObjectNoData(): Unit = {
    conf = new Configuration()
  }

}
