package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.implicits._
import geotrellis.raster._
import geotrellis.raster.Raster
import geotrellis.raster.summary.GridVisitor
import org.globalforestwatch.summarystats.Summary
import org.globalforestwatch.util.Geodesy

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
}

object GfwProDashboardSummary {
  // GfwProDashboardSummary from Raster[GfwProDashboardTile] -- cell types may not be the same

  def getGridVisitor(
                      kwargs: Map[String, Any]
                    ): GridVisitor[Raster[GfwProDashboardTile], GfwProDashboardSummary] =
    new GridVisitor[Raster[GfwProDashboardTile], GfwProDashboardSummary] {
      private var acc: GfwProDashboardSummary =
        new GfwProDashboardSummary()

      def result: GfwProDashboardSummary = acc

      def visit(raster: Raster[GfwProDashboardTile],
                col: Int,
                row: Int): Unit = {

        // This is a pixel by pixel operation
        val gladAlertsCoverage: Boolean =
          raster.tile.gladAlerts.getCoverage(col, row)
        raster.tile.gladAlerts.getData(col, row)
        val gladAlerts: Option[(String, Boolean)] =
          raster.tile.gladAlerts.getData(col, row)

        val alertDate: Option[String] = {
          gladAlerts match {
            case Some((date, _)) => Some(date)
            case _ => None
          }
        }

        val groupKey =
          GfwProDashboardRawDataGroup(gladAlertsCoverage, alertDate)

        val summaryData: GfwProDashboardRawData =
          acc.stats
            .getOrElse(key = groupKey, default = GfwProDashboardRawData(0))

        if (alertDate.isDefined) summaryData.alertCount += 1

        val new_stats = acc.stats.updated(groupKey, summaryData)
        acc = GfwProDashboardSummary(new_stats)

      }

    }

}
