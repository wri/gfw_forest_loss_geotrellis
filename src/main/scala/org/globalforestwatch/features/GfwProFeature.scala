package org.globalforestwatch.features

import org.apache.spark.sql.functions.col
import org.globalforestwatch.summarystats.SummaryCommand
import org.apache.spark.sql.Column

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

  case class Filter(
    base: Option[SummaryCommand.BaseFilter],
    id: Option[SummaryCommand.FeatureIdFilter]
  ) extends FeatureFilter {
    def filterConditions: List[Column] = {
      // TODO: is "iso" column same as "iso3"? gadm.isoFirst was applied to "iso" before
      // TODO: add wdpaID filter
      base.toList.flatMap(_.filters()) ++
        id.toList.flatMap(_.filters(idColumn=col("location_id")))
    }
  }
}
