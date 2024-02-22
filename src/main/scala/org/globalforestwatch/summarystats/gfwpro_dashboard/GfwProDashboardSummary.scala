package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.implicits._
import geotrellis.raster._
import geotrellis.raster.Raster
import geotrellis.raster.summary.GridVisitor
import org.globalforestwatch.summarystats.Summary
import org.globalforestwatch.util.Geodesy
import java.time.LocalDate

/** LossData Summary by year */
case class GfwProDashboardSummary(
                                   stats: Map[GfwProDashboardRawDataGroup, GfwProDashboardRawData] = Map.empty
                                 ) extends Summary[GfwProDashboardSummary] {

  /** Combine two Maps and combine their LossData when a year is present in both */
  def merge(other: GfwProDashboardSummary): GfwProDashboardSummary = {
    // the years.combine method uses LossData.lossDataSemigroup instance to perform per value combine on the map
    GfwProDashboardSummary(stats.combine(other.stats))
  }
  def isEmpty = stats.isEmpty

  def toGfwProDashboardData(): GfwProDashboardData = {
    stats
      .map { case (group, data) => group.
        toGfwProDashboardData(data.alertCount, data.treeCoverExtentArea) }
      .foldLeft(GfwProDashboardData.empty)( _ merge _)
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
        val integratedAlertDate: Option[LocalDate] = raster.tile.integratedAlerts.getData(col, row).map { case (date, _) => date }
        val integratedAlertCoverage = raster.tile.integratedAlerts.t.isDefined
        val isTreeCoverExtent30: Boolean = tcd2000 > 30

        val groupKey = GfwProDashboardRawDataGroup(integratedAlertDate, integratedAlertsCoverage = integratedAlertCoverage)
        val summaryData = acc.stats.getOrElse(groupKey, GfwProDashboardRawData(treeCoverExtentArea = 0.0, alertCount = 0))

        if (isTreeCoverExtent30) {
          val re: RasterExtent = raster.rasterExtent
          val areaHa = Geodesy.pixelArea(lat = re.gridRowToMap(row), re.cellSize) / 10000.0
          summaryData.treeCoverExtentArea += areaHa
        }

        if (integratedAlertDate.isDefined) {
          summaryData.alertCount += 1
        }

        val new_stats = acc.stats.updated(groupKey, summaryData)
        acc = GfwProDashboardSummary(new_stats)
      }
    }
}
