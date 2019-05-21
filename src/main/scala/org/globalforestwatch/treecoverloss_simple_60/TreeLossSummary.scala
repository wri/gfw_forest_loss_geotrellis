package org.globalforestwatch.treecoverloss_simple_60

import geotrellis.contrib.polygonal.CellVisitor
import geotrellis.raster._
import cats.implicits._
import org.globalforestwatch.util.Geodesy
import geotrellis.raster.histogram.StreamingHistogram

/** LossData Summary by year */
case class TreeLossSummary(
  stats: Map[TreeLossDataGroup, TreeLossData] = Map.empty
) {

  /** Combine two Maps and combine their LossData when a year is present in both */
  def merge(other: TreeLossSummary): TreeLossSummary = {
    // the years.combine method uses LossData.lossDataSemigroup instance to perform per value combine on the map
    TreeLossSummary(stats.combine(other.stats))
  }
}

object TreeLossSummary {
  // TreeLossSummary form Raster[TreeLossTile] -- cell types may not be the same

  implicit val mdhCellRegisterForTreeLossRaster1
    : CellVisitor[Raster[TreeLossTile], TreeLossSummary] =
    new CellVisitor[Raster[TreeLossTile], TreeLossSummary] {

      def register(raster: Raster[TreeLossTile],
                   col: Int,
                   row: Int,
                   acc: TreeLossSummary): TreeLossSummary = {
        // This is a pixel by pixel operation
        val loss: Integer = raster.tile.loss.getData(col, row)
        val tcd2010: Boolean = raster.tile.tcd2010.getData(col, row)

        val cols: Int = raster.rasterExtent.cols
        val rows: Int = raster.rasterExtent.rows
        val ext = raster.rasterExtent.extent
        val cellSize = raster.cellSize

        val lat: Double = raster.rasterExtent.gridRowToMap(row)
        val area: Double = Geodesy.pixelArea(lat, raster.cellSize) // uses Pixel's center coordiate.  +- raster.cellSize.height/2 doesn't make much of a difference

        val areaHa = area / 10000.0

        val pKey = TreeLossDataGroup(60)

        val summary: TreeLossData =
          acc.stats.getOrElse(
            key = pKey,
            default = TreeLossData(TreeLossYearDataMap.empty, 0, 0)
          )

        summary.totalArea += areaHa

        if (tcd2010) {

          if (loss != null) {
            summary.lossYear(loss).area_loss += areaHa
          }

          summary.extent2010 += areaHa

        }

        val updated_summary: Map[TreeLossDataGroup, TreeLossData] =
          acc.stats.updated(pKey, summary)

        TreeLossSummary(updated_summary)

      }
    }
}
