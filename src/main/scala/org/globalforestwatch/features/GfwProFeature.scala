package org.globalforestwatch.features

import geotrellis.vector.Geometry
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.util.GeotrellisGeometryValidator.makeValidGeom
import org.globalforestwatch.util.Util._

object GfwProFeature extends Feature {

  val listIdPos = 0
  val locationIdPos = 1
  val geomPos = 2

  val featureIdExpr = "list_id as listId, cast(location_id as int) as locationId, ST_X(ST_Centroid(ST_GeomFromWKB(geom))) as x, ST_Y(ST_Centroid(ST_GeomFromWKB(geom))) as y"


  def getFeatureId(i: Array[String], parsed: Boolean = false): FeatureId = {

    val listId: String = i(0)
    val locationId: Int = i(1).toInt
    val x: Double = i(2).toDouble
    val y: Double = i(3).toDouble

    GfwProFeatureId(listId, locationId, x, y)
  }

  override def custom_filter(
                              filters: Map[String, Any]
                            )(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val idStart: Option[Int] = getAnyMapValue[Option[Int]](filters, "idStart")
    //    val idEnd: Option[Int] = getAnyMapValue[Option[Int]](filters, "idEnd")

    //    val idStartDF: DataFrame =
    idStart.foldLeft(df)((acc, i) => acc.filter($"location_id" >= i))

    //    idEnd.foldLeft(idStartDF)((acc, i) => acc.filter($"fid" < i))

  }
}
