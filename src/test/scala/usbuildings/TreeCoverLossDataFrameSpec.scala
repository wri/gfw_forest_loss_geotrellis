package usbuildings

import geotrellis.spark.testkit.TestEnvironment

import org.scalatest._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


class TreeCoverLossDataFrameSpec extends FunSpec with TestEnvironment {
  override lazy val _ssc: SparkSession = {
    val conf = new SparkConf()
    conf
      .setMaster("local[*]")
      .setAppName("Test Context")
      .set("spark.default.parallelism", "8")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
      .set("spark.kryoserializer.buffer.max", "500m")

    SparkSession.builder().config(conf).getOrCreate()
  }

  val spark: SparkSession = this._ssc

  val arr: Array[String] = Array("50N_080W")
  val rddOrUris: RDD[String] = spark.sparkContext.parallelize(arr, arr.length)

  val rddOfRasterSource = rddOrUris.map { grid =>
    RasterUtils.gridToRasterSources(grid)
  }
  val rddOfRasterTiles = rddOfRasterSource.flatMap { case (rs1, rs2, rs3) =>
    RasterUtils.rasterTiles(rs1, rs2, rs3)
  }.repartition(arr.length * 100)

  val rddOfTuples = rddOfRasterTiles.flatMap{ case (r1, r2, r3) =>
    RasterUtils.rasterAsTable(r1, r2, r3)
  }

  import spark.sqlContext.implicits._
  val dataframe: DataFrame = rddOfTuples.toDF("col", "row", "loss_year", "tcd", "co2")

  it("is able to read the raster tiles for grid 50N_080W") {
    val count = rddOfRasterTiles.count()
    info(s"read: $count tiles x 3")
  }

}
