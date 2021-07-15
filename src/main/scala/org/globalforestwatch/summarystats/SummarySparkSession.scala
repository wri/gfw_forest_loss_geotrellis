package org.globalforestwatch.summarystats

//import org.apache.sedona.core.serde.SedonaKryoRegistrator
//import org.apache.sedona.sql.utils.SedonaSQLRegistrator

import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object SummarySparkSession {

  def apply(name: String): SparkSession = {
    val conf: SparkConf = new SparkConf()
      .setAppName(name)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
      .set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .set("spark.debug.maxToStringFields", "255")
      .set("geospark.join.gridtype", "kdbtree")
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
