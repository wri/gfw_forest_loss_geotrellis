package org.globalforestwatch.treecoverloss

import org.apache.spark.SparkConf
import org.apache.spark.sql._

case class TreeLossSparkSession() {

  val conf: SparkConf = new SparkConf()
    .setIfMissing("spark.master", "local[*]")
    .setAppName("Tree Cover Loss DataFrame")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
    .set("spark.sql.crossJoin.enabled", "true")

  implicit val spark: SparkSession =
    SparkSession.builder.config(conf).getOrCreate

}
