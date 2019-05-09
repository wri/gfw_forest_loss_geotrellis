package org.globalforestwatch.gladalerts

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object GladAlertsSparkSession {

  val conf: SparkConf = new SparkConf()
    .setAppName("Glad Alerts DataFrame")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
//    .set("spark.sql.crossJoin.enabled", "true")

  val localConf: SparkConf = conf
    .setIfMissing("spark.master", "local[*]")

  implicit val spark: SparkSession = try {
    SparkSession.builder.config(conf).getOrCreate
  } catch {
    case i: java.lang.ExceptionInInitializerError =>
      SparkSession.builder.config(localConf).getOrCreate
    case e: Throwable => throw e
  }

}
