package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.implicits._
import geotrellis.raster._
import geotrellis.raster.Raster
import geotrellis.raster.summary.GridVisitor
import org.globalforestwatch.summarystats.Summary
import org.globalforestwatch.util.Geodesy
import org.globalforestwatch.util.Util
import java.time.LocalDate

/** GfwProDashboardRawData broken down by GfwProDashboardRawDataGroup, which includes
  * alert date and confidence, but lots of other characteristics as well. */
case class GfwProDashboardSummary(
                                   stats: Map[GfwProDashboardRawDataGroup, GfwProDashboardRawData] = Map.empty
                                 ) extends Summary[GfwProDashboardSummary] {

  /** Combine two Maps by combining GfwProDashboardRawDataGroup entries that have the
    * same values. This merge function is used by summaryStats.summarySemigroup to
    * define a combine operation on GfwProDashboardSummary, which is used to combine
    * records with the same FeatureId in ErrorSummaryRDD. */
  def merge(other: GfwProDashboardSummary): GfwProDashboardSummary = {
    // the stats.combine method uses the GfwProDashboardRawData.lossDataSemigroup
    // instance to perform per-value combine on the map.
    GfwProDashboardSummary(stats.combine(other.stats))
  }
  def isEmpty = stats.isEmpty

  /** Pivot raw data to GfwProDashboardData and aggregate across alert dates. */
  def toGfwProDashboardData(ignoreGadm: Boolean): List[GfwProDashboardData] = {
    if (ignoreGadm) {
      // Combine all GfwProDashboardData results ignoring different groupGadmIds.
      List(stats
        .map { case (group, data) => group.
          toGfwProDashboardData(data.alertCount, data.treeCoverExtentArea) }
        .foldLeft(GfwProDashboardData.empty)( _ merge _))
    } else {
      // Combine all GfwProDashboardData results into separate rows based on groupGadmI
      stats
        .groupBy { case(group, data) => group.groupGadmId }
        .map { case(key, list) =>
          list.map { case (group, data) => group.
            toGfwProDashboardData(data.alertCount, data.treeCoverExtentArea) }
            .foldLeft(GfwProDashboardData.empty)(_ merge _)
        }.toList
    }
  }
}

object GfwProDashboardSummary {

  def getGridVisitor(kwargs: Map[String, Any]): GridVisitor[Raster[GfwProDashboardTile], GfwProDashboardSummary] =
    new GridVisitor[Raster[GfwProDashboardTile], GfwProDashboardSummary] {
      private var acc: GfwProDashboardSummary =
        new GfwProDashboardSummary()

      def result: GfwProDashboardSummary = acc

      def visit(raster: Raster[GfwProDashboardTile], col: Int, row: Int): Unit = {
        val tcd2000: Integer = raster.tile.tcd2000.getData(col, row)
        val integratedAlertDateAndConf: Option[(LocalDate, Int)] = raster.tile.integratedAlerts.getData(col, row)
        val integratedAlertCoverage = raster.tile.integratedAlerts.t.isDefined
        val isTreeCoverExtent30: Boolean = tcd2000 > 30
        val naturalForestCategory: String = raster.tile.sbtnNaturalForest.getData(col, row)
        val jrcForestCover: Boolean = raster.tile.jrcForestCover.getData(col, row)

        val gadmId: String = if (kwargs("getRasterGadm") == true) {
          val gadmAdm0: String = raster.tile.gadm0.getData(col, row)
          // Skip processing this pixel if gadmAdm0 is empty
          if (gadmAdm0 == "") {
            return
          }
          val gadmAdm1: Integer = raster.tile.gadm1.getData(col, row)
          val gadmAdm2: Integer = raster.tile.gadm2.getData(col, row)
          Util.getGadmId(gadmAdm0, gadmAdm1, gadmAdm2, kwargs("gadmVers").asInstanceOf[String])
        } else {
          ""
        }


        val groupKey = GfwProDashboardRawDataGroup(gadmId, integratedAlertDateAndConf,
          integratedAlertCoverage,
          naturalForestCategory == "Natural Forest",
          jrcForestCover,
          isTreeCoverExtent30)
        val summaryData = acc.stats.getOrElse(groupKey, GfwProDashboardRawData(treeCoverExtentArea = 0.0, alertCount = 0))

        val re: RasterExtent = raster.rasterExtent
        val areaHa = Geodesy.pixelArea(lat = re.gridRowToMap(row), re.cellSize) / 10000.0
        summaryData.treeCoverExtentArea += areaHa

        if (integratedAlertDateAndConf.isDefined) {
          summaryData.alertCount += 1
        }

        val new_stats = acc.stats.updated(groupKey, summaryData)
        acc = GfwProDashboardSummary(new_stats)
      }
    }
}
