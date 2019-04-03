package org.globalforestwatch.treecoverloss

import geotrellis.contrib.polygonal.CellVisitor
import geotrellis.raster._
import cats.implicits._


/** LossData Summary by year */
case class TreeLossSummary(stats: Map[(Int, Int, Int), LossData] = Map.empty) {
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
        val loss: Int = if (isNoData(raster.tile.loss.get(col, row))) 0 else raster.tile.loss.get(col, row)
        val gain: Int = if (isNoData(raster.tile.gain.get(col, row))) 0 else 1
        val tcd2000: Int = if (isNoData(raster.tile.tcd2000.get(col, row))) 0 else raster.tile.tcd2000.get(col, row)
     //   val tcd2010: Int = if (isNoData(raster.tile.tcd2010.get(col, row))) 0 else raster.tile.tcd2010.get(col, row)

        // If we don't have these tiles use default values for pixel
        val co2Pixel: Double = raster.tile.co2Pixel.map(_.getDouble(col, row)).getOrElse(0)
        val gadm36: Int = if (isNoData(raster.tile.gadm36.map(_.get(col, row)).getOrElse(0))) 0 else raster.tile.gadm36.map(_.get(col, row)).getOrElse(0)

        val tcd2000Thresh: Int = TreeCoverDensity2000.lookup(tcd2000)
    //    val tcd2010Thresh: Int = TreeCoverDensity.threshold(tcd2010)

        val lat:Double = raster.rasterExtent.gridRowToMap(row)
        val area: Double = Geodesy.pixelArea(lat, raster.cellSize)

        val gainArea: Double = gain * area

        val pKey = (loss, tcd2000Thresh, gadm36) // tcd2010Thresh, gadm36)

        val summary: LossData = acc.stats.getOrElse(
          key = pKey,
          default = LossData(0, 0, 0))

        summary.totalArea += area
        summary.totalCo2 += co2Pixel
        summary.totalGainArea += gainArea

        val updated_summary: Map[(Int, Int, Int), LossData] = acc.stats.updated(pKey, summary)

        TreeLossSummary(updated_summary)
      }
    }
}
