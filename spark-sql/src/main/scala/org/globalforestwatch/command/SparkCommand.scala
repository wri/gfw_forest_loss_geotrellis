package org.globalforestwatch.command

import org.apache.spark.sql.SparkSession
import org.locationtech.rasterframes._

trait SparkCommand {
  implicit lazy val spark: SparkSession = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(getClass().getSimpleName())
      .config("spark.default.parallelism", 200)
      .config("spark.sql.shuffle.partitions", 200)
      .withKryoSerialization
      .getOrCreate()
      .withRasterFrames

    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def withSpark[A](job: SparkSession => A): A = {
    try {
      job(spark)
    } finally {
      spark.stop()
    }
  }
}
