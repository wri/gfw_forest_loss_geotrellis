package org.globalforestwatch.carbonflux

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object CarbonFluxSparkSession {

  def apply(): SparkSession = {

    val conf: SparkConf = new SparkConf()
      .setAppName("Carbon Flux DataFrame")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
      .set("spark.sql.crossJoin.enabled", "true")

    val localConf: SparkConf = conf
      .setIfMissing("spark.master", "local[*]")

    val localConf2: SparkConf = localConf
      .set("spark.driver.bindAddress", "127.0.0.1")

    implicit val spark: SparkSession = try {
      SparkSession.builder.config(conf).getOrCreate
    } catch {
      case i: java.lang.ExceptionInInitializerError =>
        SparkSession.builder.config(localConf).getOrCreate
      case b: java.net.BindException =>
        SparkSession.builder.config(localConf2).getOrCreate
      case e: Throwable => throw e
    }
    spark
  }
}
