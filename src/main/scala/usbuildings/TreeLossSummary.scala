package usbuildings

import geotrellis.contrib.polygonal.CellVisitor
import geotrellis.raster.{CellGrid, CellType, MultibandRaster, MultibandTile, Raster, Tile, isData}
import geotrellis.raster.histogram.StreamingHistogram
import geotrellis.vector.Extent

/** Summary data per class
  * Note: This case class contains mutable values
  * @param tcd distribution of tree cover density pixels values
  * @param totalCo2 sum of co2 pixel values
  */
case class LossData(tcd: StreamingHistogram, var totalCo2: Double)

/** LossData Summary per year */
case class TreeLossSummary(years: Map[Int, LossData] = Map.empty)

object TreeLossSummary {
  // TreeLossSummary form Raster[MultibandTile] -- all bands must share cell type
  implicit val mdhCellRegister = new CellVisitor[Raster[MultibandTile], TreeLossSummary] {
    override def register(raster: MultibandRaster, col: Int, row: Int, acc: TreeLossSummary): TreeLossSummary = {
      val loss_year = raster.tile.band(0).get(col, row)
      if (isData(loss_year)) {
        val tcd = raster.tile.band(1).getDouble(col, row)
        val co2 = raster.tile.band(2).getDouble(col, row)

        val year_summary: LossData = acc.years.getOrElse(loss_year, LossData(StreamingHistogram(256), 0))
        year_summary.tcd.countItem(tcd)
        year_summary.totalCo2 += co2
        val updated_summary: Map[Int, LossData] = acc.years.updated(loss_year, year_summary)
        TreeLossSummary(updated_summary)
      } else
        acc
    }
  }

  // TreeLossSummary form Raster[TreeLossTile] -- cell types may not be the same
  implicit val mdhCellRegisterForTreeLossRaster1 = new CellVisitor[Raster[TreeLossTile], TreeLossSummary] {
    override def register(raster: Raster[TreeLossTile], col: Int, row: Int, acc: TreeLossSummary): TreeLossSummary = {
      val loss_year = raster.tile.loss_year.get(col, row)
      if (isData(loss_year)) {
        val tcd = raster.tile.tcd.getDouble(col, row)
        val co2 = raster.tile.co2.getDouble(col, row)

        val year_summary: LossData = acc.years.getOrElse(loss_year, LossData(StreamingHistogram(256), 0))
        year_summary.tcd.countItem(tcd)
        year_summary.totalCo2 += co2
        val updated_summary: Map[Int, LossData] = acc.years.updated(loss_year, year_summary)
        TreeLossSummary(updated_summary)
      } else
        acc
    }
  }
}