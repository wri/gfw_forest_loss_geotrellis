package org.globalforestwatch.features

import geotrellis.vector.Geometry
import geotrellis.vector.io.wkb.WKB
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.globalforestwatch.util.GeometryReducer

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

  def filter(filters: Map[String, Any])(df: DataFrame): DataFrame = {
    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val isoFirst: Option[String] = getMapValue(filters, "isoFirst")
    val isoStart: Option[String] = getMapValue(filters, "isoStart")
    val isoEnd: Option[String] = getMapValue(filters, "isoEnd")
    val iso: Option[String] = getMapValue(filters, "iso")
    val wdpaIdStart: Option[Int] = getMapValue(filters, "wdpaIdStart")
    val wdpaIdEnd: Option[Int] = getMapValue(filters, "wdpaIdEnd")
    val iucnCat: Option[String] = getMapValue(filters, "iucnCat")
    val wdpaStatus: Option[String] = getMapValue(filters, "wdpaStatus")
    val limit: Option[Int] = getMapValue(filters, "limit")
    val tcl: Boolean = getMapValue(filters, "tcl")
    val glad: Boolean = getMapValue(filters, "glad")

    var newDF = df

    if (glad) newDF = newDF.filter($"glad" === "t")

    if (tcl) newDF = newDF.filter($"tcl" === "t")

    isoFirst.foreach { firstLetter =>
      newDF = newDF.filter(substring($"iso", 0, 1) === firstLetter(0))
    }

    isoStart.foreach { startCode =>
      newDF = newDF.filter($"iso" >= startCode)
    }

    isoEnd.foreach { endCode =>
      newDF = newDF.filter($"iso" < endCode)
    }

    iso.foreach { isoCode =>
      newDF = newDF.filter($"iso" === isoCode)
    }

    wdpaIdStart.foreach { startId =>
      newDF = newDF.filter($"wdpaid" >= startId)
    }

    wdpaIdEnd.foreach { endId =>
      newDF = newDF.filter($"wdpaid" < endId)
    }

    iucnCat.foreach { category =>
      newDF = newDF.filter($"iucn_cat" === category)
    }

    wdpaStatus.foreach { status =>
      newDF = newDF.filter($"status" === status)
    }

    limit.foreach { n =>
      newDF = newDF.limit(n)
    }

    newDF
  }
}
