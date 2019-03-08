package org.globalforestwatch.treecoverloss

import geotrellis.contrib.polygonal.CellVisitor
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.histogram.StreamingHistogram
import cats.implicits._

/** LossData Summary by year */
case class TreeLossSummary(years: Map[Int, LossData] = Map.empty) {
  /** Combine two Maps and combine their LossData when a year is present in both */
  def merge(other: TreeLossSummary): TreeLossSummary = {
    // the years.combine method uses LossData.lossDataSemigroup instance to perform per value combine on the map
    TreeLossSummary(years.combine(other.years))
  }
}

object TreeLossSummary {
  // TreeLossSummary form Raster[TreeLossTile] -- cell types may not be the same
  implicit val mdhCellRegisterForTreeLossRaster1 =
    new CellVisitor[Raster[TreeLossTile], TreeLossSummary] {
      def register(raster: Raster[TreeLossTile], col: Int, row: Int, acc: TreeLossSummary): TreeLossSummary = {

        // This is a pixel by pixel operation
        val loss_year = raster.tile.lossYear.get(col, row)
        if (isData(loss_year)) {
          val tcd = raster.tile.treeCoverDensity.getDouble(col, row)
          val co2 = raster.tile.biomass.getDouble(col, row)

          val year_summary: LossData = acc.years.getOrElse(
            key = loss_year,
            default = LossData(StreamingHistogram(size = 256), 0))

          year_summary.tcdHistogram.countItem(tcd)
          year_summary.totalCo2 += co2

          val updated_summary: Map[Int, LossData] = acc.years.updated(loss_year, year_summary)

          TreeLossSummary(updated_summary)
        } else {
          acc
        }
      }
    }
}