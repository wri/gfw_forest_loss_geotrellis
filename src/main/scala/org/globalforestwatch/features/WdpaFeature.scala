package org.globalforestwatch.features

import org.apache.spark.sql.functions.col
import org.globalforestwatch.summarystats.SummaryCommand
import org.apache.spark.sql.Column

object WdpaFeature extends Feature {

  val siteIdPos = 0
  val namePos = 1
  val iucnCatPos = 2
  val isoPos = 3
  val statusPos = 4
  val geomPos = 7
  val featureCount = 4

  val featureIdExpr =
    "cast(site_id as int) as siteId, name as name, iucn_cat as iucnCat, iso3 as iso, status"

  def getFeatureId(i: Array[String], parsed: Boolean = false): FeatureId = {

    val siteId: Int = i(siteIdPos).toInt
    val name: String = i(namePos)
    val iucnCat: String = i(iucnCatPos)
    val iso: String = i(isoPos)
    val status: String = i(statusPos)

    WdpaFeatureId(siteId, name, iucnCat, iso, status)
  }

  case class Filter(
    base: Option[SummaryCommand.BaseFilter],
    gadm: Option[SummaryCommand.GadmFilter],
    wdpa: Option[SummaryCommand.WdpaFilter]
  ) extends FeatureFilter {
    def filterConditions: List[Column]= {
      // TODO: is "iso" column same as "iso3"? gadm.isoFirst was applied to "iso" before
      base.toList.flatMap(_.filters()) ++
        wdpa.toList.flatMap(_.filters()) ++
        gadm.toList.flatMap(_.filters(isoColumn=col("iso3"), admin1Column=col("admin1"), admin2Column=col("admin2")))
    }
  }
}
