package org.globalforestwatch.summarystats.treecoverloss

import cats.data.NonEmptyList
import cats.implicits._
import geotrellis.contrib.polygonal.CellVisitor
import geotrellis.raster._
import org.globalforestwatch.summarystats.Summary
import org.globalforestwatch.util.Geodesy
import org.globalforestwatch.util.Util.getAnyMapValue
import org.globalforestwatch.util.Implicits._

import scala.annotation.tailrec

/** LossData Summary by year */
case class TreeLossSummary(stats: Map[TreeLossDataGroup, TreeLossData] =
                           Map.empty,
                           kwargs: Map[String, Any])
  extends Summary[TreeLossSummary] {

  /** Combine two Maps and combine their LossData when a year is present in both */
  def merge(other: TreeLossSummary): TreeLossSummary = {
    // the years.combine method uses LossData.lossDataSemigroup instance to perform per value combine on the map
    TreeLossSummary(stats.combine(other.stats), kwargs)
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

        // val tcdYear: Int = getAnyMapValue[Int](acc.kwargs, "tcdYear")

        // This is a pixel by pixel operation
        val loss: Integer = raster.tile.loss.getData(col, row)
        val tcd2000: Integer = raster.tile.tcd2000.getData(col, row)
        val primaryForest: Boolean = raster.tile.primaryForest.getData(col, row)
        val ifl: Boolean = raster.tile.ifl.getData(col, row)
        val peatlands: Boolean = raster.tile.peatlands.getData(col, row)
        val protectedAreas: String = raster.tile.protectedAreas.getData(col, row)

        //        val cols: Int = raster.rasterExtent.cols
        //        val rows: Int = raster.rasterExtent.rows
        //        val ext = raster.rasterExtent.extent
        //        val cellSize = raster.cellSize

        val lat: Double = raster.rasterExtent.gridRowToMap(row)
        val area: Double = Geodesy.pixelArea(lat, raster.cellSize) // uses Pixel's center coordiate.  +- raster.cellSize.height/2 doesn't make much of a difference

        val areaHa = area / 10000.0

        // val thresholds: List[Int] =
        //  getAnyMapValue[NonEmptyList[Int]](acc.kwargs, "thresholdFilter").toList

        // @tailrec
        def updateSummary(
                           stats: Map[TreeLossDataGroup, TreeLossData]
         ): Map[TreeLossDataGroup, TreeLossData] = {
          val pKey = TreeLossDataGroup()
          val summary: TreeLossData =
            stats.getOrElse(
              key = pKey,
              default =
                TreeLossData(TreeLossYearDataMap.empty, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
            )

            val primaryForestArea = areaHa * primaryForest
            val iflArea = areaHa * ifl
            val peatlandsArea = areaHa * peatlands
            val wdpaArea = areaHa * (!protectedAreas.isEmpty)

            summary.totalArea += areaHa
            summary.primaryForestExtent += primaryForestArea
            summary.iflExtent += iflArea
            summary.peatlandsExtent += peatlandsArea
            summary.wdpaExtent += wdpaArea

            if (tcd2000 >= 30) {
              summary.treecoverExtent2000 += areaHa

              if (loss != null) {
                summary.totalLossArea += areaHa
                summary.totalPrimaryForestLoss += primaryForestArea
                summary.totalIflLoss += iflArea
                summary.totalPeatlandsLoss += peatlandsArea
                summary.totalWdpaLoss += wdpaArea

                summary.lossYear(loss).treecoverLoss += areaHa
                summary.lossYear(loss).primaryLoss += primaryForestArea
                summary.lossYear(loss).iflLoss += iflArea
                summary.lossYear(loss).peatlandsLoss += peatlandsArea
                summary.lossYear(loss).wdpaLoss += wdpaArea
              }
            }

          stats.updated(pKey, summary)
        }

        val updatedSummary: Map[TreeLossDataGroup, TreeLossData] =
          updateSummary(acc.stats)

        TreeLossSummary(updatedSummary, acc.kwargs)

      }
    }
}
