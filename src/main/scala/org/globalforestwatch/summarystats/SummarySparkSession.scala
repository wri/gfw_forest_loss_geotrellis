package org.globalforestwatch.summarystats

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

object SummarySparkSession {

  def apply(name: String): SparkSession = {
    val conf: SparkConf = new SparkConf()
      .setAppName(name)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
      .set("spark.debug.maxToStringFields", "255")
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

    GeoSparkSQLRegistrator.registerAll(spark)

    spark
  }
}
