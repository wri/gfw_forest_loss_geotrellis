package org.globalforestwatch.summarystats

//import org.apache.sedona.core.serde.SedonaKryoRegistrator
//import org.apache.sedona.sql.utils.SedonaSQLRegistrator

import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object SummarySparkSession {

  def apply(name: String): SparkSession = {
    val conf: SparkConf = new SparkConf()
      .setAppName(name)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
      .set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
      .set("spark.debug.maxToStringFields", "255")
      .set("sedona.join.gridtype", "quadtree")
      .set("spark.hadoop.fs.s3a.connection.maximum", "100")
      .set("spark.hadoop.fs.s3a.retry.limit", "20")
      .set("spark.hadoop.fs.s3a.connection.timeout", "200000")
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

    SedonaSQLRegistrator.registerAll(spark)

    spark
  }

  def run(name: String)(job: SparkSession => Unit): Unit = {
    val spark = apply(name)
    try {
      job(spark)
    } finally {
      spark.stop()
    }
  }
}
