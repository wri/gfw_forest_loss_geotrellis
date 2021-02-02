package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.implicits._
import geotrellis.raster._
import geotrellis.raster.summary.GridVisitor
import org.globalforestwatch.summarystats.Summary
import org.globalforestwatch.util.Geodesy

/** LossData Summary by year */
case class ForestChangeDiagnosticSummary(
  stats: ForestChangeDiagnosticData = ForestChangeDiagnosticData.empty
) extends Summary[ForestChangeDiagnosticSummary] {

  /** Combine two Maps and combine their LossData when a year is present in both */
  def merge(
    other: ForestChangeDiagnosticSummary
  ): ForestChangeDiagnosticSummary = {
    // the years.combine method uses LossData.lossDataSemigroup instance to perform per value combine on the map
    ForestChangeDiagnosticSummary(stats.combine(other.stats))
  }
}

object ForestChangeDiagnosticSummary {
  // ForestChangeDiagnosticSummary form Raster[ForestChangeDiagnosticTile] -- cell types may not be the same

  def getGridVisitor(
    kwargs: Map[String, Any]
  ): GridVisitor[Raster[ForestChangeDiagnosticTile],
                 ForestChangeDiagnosticSummary] =
    new GridVisitor[Raster[ForestChangeDiagnosticTile], ForestChangeDiagnosticSummary] {
      private var acc: ForestChangeDiagnosticSummary =
        new ForestChangeDiagnosticSummary()

      def result: ForestChangeDiagnosticSummary = acc

      def visit(raster: Raster[ForestChangeDiagnosticTile],
                col: Int,
                row: Int): Unit = {

        // This is a pixel by pixel operation
        // We only look at loss for TCD > 30
        val tcd2000: Integer = raster.tile.tcd2000.getData(col, row)
        val lossYear: Int = {
          val loss = raster.tile.loss.getData(col, row)
          if (loss != null) {
            loss.toInt
          } else { 0 }
        }

        if (tcd2000 > 30 && lossYear > 0) {

          val lat: Double = raster.rasterExtent.gridRowToMap(row)
          val area: Double = Geodesy.pixelArea(lat, raster.cellSize) // uses Pixel's center coordiate.  +- raster.cellSize.height/2 doesn't make much of a difference
          val areaHa = area / 10000.0

          // contextual layers

          val isPrimaryForest: Boolean =
            raster.tile.isPrimaryForest.getData(col, row)
          val isPeatlands: Boolean = raster.tile.isPeatlands.getData(col, row)
          val isIntactForestLandscapes2016: Boolean =
            raster.tile.isIntactForestLandscapes2016.getData(col, row)
          val wdpa: String = raster.tile.wdpaProtectedAreas.getData(col, row)
          val seAsiaLandCover: String =
            raster.tile.seAsiaLandCover.getData(col, row)
          val idnLandCover: String = raster.tile.idnLandCover.getData(col, row)
          val isSoyPlantedAreas: Boolean = raster.tile.isSoyPlantedArea.getData(col, row)
          val idnForestArea: String = raster.tile.idnForestArea.getData(col, row)
          val isIDNForestMoratorium: Boolean = raster.tile.isIDNForestMoratorium.getData(col, row)

          // summary statistics
          val treeCoverLossTotalYearly = ForestChangeDiagnosticTCLYearly.fill(lossYear, areaHa)
          val treeCoverLossPrimaryForestYearly = ForestChangeDiagnosticTCLYearly.fill(lossYear, areaHa, isPrimaryForest)
          val treeCoverLossPeatlandYearly = ForestChangeDiagnosticTCLYearly.fill(lossYear, areaHa, isPeatlands)
          val treeCoverLossIntactForestYearly = ForestChangeDiagnosticTCLYearly.fill(lossYear, areaHa, isIntactForestLandscapes2016)
          val treeCoverLossProtectedAreasYearly = ForestChangeDiagnosticTCLYearly.fill(lossYear, areaHa, (wdpa == "Category Ia/b or II"))
          val treeCoverLossSEAsiaLandCoverYearly =
            ForestChangeDiagnosticTCLClassYearly.fill(seAsiaLandCover, lossYear, areaHa, "Unknown")
          val treeCoverLossIDNLandCoverYearly =
            ForestChangeDiagnosticTCLClassYearly.fill(idnLandCover, lossYear, areaHa, "")
          val treeCoverLossSoyPlantedAreasYearly = ForestChangeDiagnosticTCLYearly.fill(lossYear, areaHa, isSoyPlantedAreas)
          val treeCoverLossIDNForestAreaYearly = ForestChangeDiagnosticTCLClassYearly.fill(idnForestArea, lossYear, areaHa, "")
          val treeCoverLossIDNForestMoratoriumYearly = ForestChangeDiagnosticTCLYearly.fill(lossYear, areaHa, isIDNForestMoratorium)

          // Combine results
          val newStats = ForestChangeDiagnosticData(
            treeCoverLossTotalYearly,
            treeCoverLossPrimaryForestYearly,
            treeCoverLossPeatlandYearly,
            treeCoverLossIntactForestYearly,
            treeCoverLossProtectedAreasYearly,
            treeCoverLossSEAsiaLandCoverYearly,
            treeCoverLossIDNLandCoverYearly,
            treeCoverLossSoyPlantedAreasYearly,
            treeCoverLossIDNForestAreaYearly,
            treeCoverLossIDNForestMoratoriumYearly
          )

          acc = ForestChangeDiagnosticSummary(acc.stats.merge(newStats))

        }
      }
    }
}
