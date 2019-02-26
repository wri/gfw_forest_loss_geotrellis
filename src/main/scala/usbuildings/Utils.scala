package usbuildings

import geotrellis.vector.{Feature, Polygon, Geometry}
import geotrellis.vector.io._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import java.net.URL

object Utils {
  /** Read features from GeoJSON file to generate sample CSV file */
  def createTestGeometryFile(url: String)(implicit spark: SparkSession): DataFrame = {
    val arr: Array[String] = Array(url)

    val rdd: RDD[String] = spark.sparkContext.parallelize(arr)

    val rddPolygons: RDD[(String, Int, String)] =
      rdd.flatMap { fileUrl =>
        Building.
          readFromGeoJson(new URL(fileUrl)).
          zipWithIndex.
          map { case (polygon, index) =>
            ("vermont", index, polygon.toWKT)
          }
      }

    import spark.sqlContext.implicits._
    val dataframe: DataFrame = rddPolygons.toDF("state", "index", "polygon")

    // dataframe.write.option("header", true).csv("/Users/eugene/sample")
    dataframe
  }
}