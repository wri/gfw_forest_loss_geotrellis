package org.globalforestwatch.features

import geotrellis.vector.Geometry
import geotrellis.vector.io.wkb.WKB
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.globalforestwatch.util.GeometryReducer
import org.globalforestwatch.util.Util._

object WdpaFeature extends Feature {

  val wdpaIdPos = 0
  val namePos = 1
  val iucnCatPos = 2
  val isoPos = 3
  val statusPos = 4
  val geomPos = 7

  def getFeature(i: Row): geotrellis.vector.Feature[Geometry, WdpaFeatureId] = {
    val wdpa_id: Int = i.getString(wdpaIdPos).toInt
    val name: String = i.getString(namePos)
    val iucn_cat: String = i.getString(iucnCatPos)
    val iso: String = i.getString(isoPos)
    val status: String = i.getString(statusPos)
    val geom: Geometry =
      GeometryReducer.reduce(GeometryReducer.gpr)(
        WKB.read(i.getString(geomPos))
      )
    geotrellis.vector
      .Feature(geom, WdpaFeatureId(wdpa_id, name, iucn_cat, iso, status))
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
    val wdpaIdEnd: Option[Int] = getAnyMapValue[Option[Int]](filters, "idEnd")
    val iucnCat: Option[String] =
      getAnyMapValue[Option[String]](filters, "iucnCat")
    val wdpaStatus: Option[String] =
      getAnyMapValue[Option[String]](filters, "wdpaStatus")

    val isoFirstDF: DataFrame = isoFirst.foldLeft(df)(
      (acc, i) => acc.filter(substring($"gid_0", 0, 1) === i(0))
    )
    val isoStartDF: DataFrame =
      isoStart.foldLeft(isoFirstDF)((acc, i) => acc.filter($"gid_0" >= i))
    val isoEndDF: DataFrame =
      isoEnd.foldLeft(isoStartDF)((acc, i) => acc.filter($"gid_0" < i))
    val isoDF: DataFrame =
      iso.foldLeft(isoEndDF)((acc, i) => acc.filter($"gid_0" === i))

    val wdpaIdStartDF =
      wdpaIdStart.foldLeft(isoDF)((acc, i) => acc.filter($"wdpaid" >= i))

    val wdpaIdEndtDF =
      wdpaIdEnd.foldLeft(wdpaIdStartDF)((acc, i) => acc.filter($"wdpaid" < i))

    val iucnCatDF =
      wdpaIdEnd.foldLeft(wdpaIdEndtDF)(
        (acc, i) => acc.filter($"iucn_cat" === i)
      )

    wdpaStatus.foldLeft(iucnCatDF)((acc, i) => acc.filter($"status" === i))

  }
}
