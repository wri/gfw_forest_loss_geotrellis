package usbuildings

import java.io.{File, FileInputStream}

import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.model.ObjectMetadata
import geotrellis.spark.io.s3.S3Client
import org.geotools.data.ogr.OGRDataStore
import org.geotools.data.ogr.bridj.BridjOGRDataStoreFactory

import scala.util.control.NonFatal
import geotrellis.vector.io._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.rdd.RDD
import java.net.URL

import geotrellis.raster._
import geotrellis.spark.SpatialKey
import geotrellis.spark.io.index.zcurve.Z2
import geotrellis.vector._


object Util {

  /** Read features from GeoJSON file to generate CSV file
    * This function was used to generate sample input for Vermont
    */
  def createTestGeometryFile(url: String)(implicit spark: SparkSession): DataFrame = {
    val arr: Array[String] = Array(url)

    val rdd: RDD[String] = spark.sparkContext.parallelize(arr)

    val rddPolygons: RDD[(String, Int, String)] =
      rdd.flatMap { fileUrl =>
        Building.
          readFromGeoJson(new URL(fileUrl)).
          zipWithIndex.
          map { case (feature, index) =>
            ("vermont", index, feature.geom.toWKT)
          }
      }

    import spark.sqlContext.implicits._
    val dataframe: DataFrame = rddPolygons.toDF("state", "index", "polygon")

    // dataframe.write.option("header", true).csv("/Users/eugene/sample")
    dataframe
  }

  /** Open GeoTools OGRDataStore */
  def getOgrDataStore(uri: String, driver: Option[String] =  None): OGRDataStore = {
    println(s"Opening: $uri")
    val factory = new BridjOGRDataStoreFactory()
    val params = new java.util.HashMap[String, String]
    params.put("DatasourceName", uri)
    driver.foreach(params.put("DriverName", _))
    factory.createDataStore(params).asInstanceOf[OGRDataStore]
  }

  def uploadFile(file: File, uri: AmazonS3URI): Unit = {
    val is = new FileInputStream(file)
    try {
      S3Client.DEFAULT.putObject(uri.getBucket, uri.getKey, is, new ObjectMetadata())
    } catch {
      case NonFatal(e) => is.close()
    } finally { is.close() }
  }

  def sortByZIndex(
    features: Seq[Feature[Polygon, FeatureId]],
    rasterExtent: RasterExtent
  ): Seq[Feature[Polygon, FeatureId]] = {
    def zindex(p: Point): Long = {
      val col = rasterExtent.mapXToGrid(p.x)
      val row = rasterExtent.mapXToGrid(p.y)
      Z2(col, row).z
    }

    features.sortBy{ feature => zindex(feature.geom.envelope.northWest) }
  }
}
