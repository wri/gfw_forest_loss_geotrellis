package org.globalforestwatch.features

import geotrellis.vector.Geometry
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.globalforestwatch.util.GeotrellisGeometryValidator.makeValidGeom
import org.globalforestwatch.util.Util._

object WdpaFeature extends Feature {

  val wdpaIdPos = 0
  val namePos = 1
  val iucnCatPos = 2
  val isoPos = 3
  val statusPos = 4
  val geomPos = 7
  val featureCount = 4

  val featureIdExpr =  "cast(wdpaid as int) as wdpaId, name as name, iucn_cat as iucnCat, iso3 as iso, status"

  def get(i: Row): geotrellis.vector.Feature[Geometry, FeatureId] = {
    val featureId = getFeatureId(i)
    val geom: Geometry = makeValidGeom(i.getString(geomPos))

    geotrellis.vector
      .Feature(geom, featureId)
  }

  def getFeatureId(i: Array[String], parsed: Boolean = false): FeatureId = {

    val wdpaId: Int = i(wdpaIdPos).toInt
    val name: String = i(namePos)
    val iucnCat: String = i(iucnCatPos)
    val iso: String = i(isoPos)
    val status: String = i(statusPos)

    WdpaFeatureId(wdpaId, name, iucnCat, iso, status)
  }

  override def custom_filter(
                              filters: Map[String, Any]
                            )(df: DataFrame): DataFrame = {
    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val isoFirst: Option[String] =
      getAnyMapValue[Option[String]](filters, "isoFirst")
    val isoStart: Option[String] =
      getAnyMapValue[Option[String]](filters, "isoStart")
    val isoEnd: Option[String] =
      getAnyMapValue[Option[String]](filters, "isoEnd")
    val iso: Option[String] = getAnyMapValue[Option[String]](filters, "iso")
    val wdpaIdStart: Option[Int] =
      getAnyMapValue[Option[Int]](filters, "idStart")
    //val wdpaIdEnd: Option[Int] = getAnyMapValue[Option[Int]](filters, "idEnd")
    //val iucnCat: Option[String] =
    //  getAnyMapValue[Option[String]](filters, "iucnCat")
    val wdpaStatus: Option[String] =
      getAnyMapValue[Option[String]](filters, "wdpaStatus")

    val isoFirstDF: DataFrame = isoFirst.foldLeft(df)(
      (acc, i) => acc.filter(substring($"iso", 0, 1) === i(0))
    )
    val isoStartDF: DataFrame =
      isoStart.foldLeft(isoFirstDF)((acc, i) => acc.filter($"iso" >= i))
    val isoEndDF: DataFrame =
      isoEnd.foldLeft(isoStartDF)((acc, i) => acc.filter($"iso" < i))
    val isoDF: DataFrame =
      iso.foldLeft(isoEndDF)((acc, i) => acc.filter($"iso" === i))

    val wdpaIdStartDF =
      wdpaIdStart.foldLeft(isoDF)((acc, i) => acc.filter($"wdpaid" >= i))

    /*
    val wdpaIdEndtDF =
      wdpaIdEnd.foldLeft(wdpaIdStartDF)((acc, i) => acc.filter($"wdpaid" < i))

    val iucnCatDF =
      wdpaIdEnd.foldLeft(wdpaIdEndtDF)(
        (acc, i) => acc.filter($"iucn_cat" === i)
      )
    */

    wdpaStatus.foldLeft(isoDF)((acc, i) => acc.filter($"status" === i))
  }
}
