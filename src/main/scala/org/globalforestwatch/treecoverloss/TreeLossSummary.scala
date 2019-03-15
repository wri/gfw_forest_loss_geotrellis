package org.globalforestwatch.treecoverloss

import geotrellis.contrib.polygonal.CellVisitor
import geotrellis.raster._
import cats.implicits._


/** LossData Summary by year */
case class TreeLossSummary(stats: Map[(Int, Int, Int, Int), LossData] = Map.empty) {
  /** Combine two Maps and combine their LossData when a year is present in both */
  def merge(other: TreeLossSummary): TreeLossSummary = {
    // the years.combine method uses LossData.lossDataSemigroup instance to perform per value combine on the map
    TreeLossSummary(stats.combine(other.stats))
  }
}

object TreeLossSummary {
  // TreeLossSummary form Raster[TreeLossTile] -- cell types may not be the same
  implicit val mdhCellRegisterForTreeLossRaster1 =
    new CellVisitor[Raster[TreeLossTile], TreeLossSummary] {
      def register(raster: Raster[TreeLossTile], col: Int, row: Int, acc: TreeLossSummary): TreeLossSummary = {


        // This is a pixel by pixel operation
        val loss: Int = raster.tile.loss.get(col, row)
        val gain: Int = raster.tile.gain.get(col, row)
        val tcd2000: Int = raster.tile.tcd2000.get(col, row)
        val tcd2010: Int = raster.tile.tcd2010.get(col, row)
        val co2Pixel: Double = raster.tile.co2Pixel.getDouble(col, row)
        val gadm36: Int = raster.tile.gadm36.get(col, row)

        val tcd2000Thresh: Int = TreeCoverDensity.threshold(tcd2000)
        val tcd2010Thresh: Int = TreeCoverDensity.threshold(tcd2010)

        val lat: Double = raster.extent.ymin + col * raster.cellSize.height
        val area: Double = Geodesy.pixelArea(lat, raster.cellSize)

        val gainArea: Double = gain * area // TODO: verify that gain is binary -> 0 | 1

        val pKey = (loss, tcd2000Thresh, tcd2010Thresh, gadm36)

        val summary: LossData = acc.stats.getOrElse(
          key = pKey,
          default = LossData(0, 0, 0))

        summary.totalArea += area
        summary.totalCo2 += co2Pixel
        summary.totalGainArea += gainArea

        val updated_summary: Map[(Int, Int, Int, Int), LossData] = acc.stats.updated(pKey, summary)

        TreeLossSummary(updated_summary)
      }
    }
}
