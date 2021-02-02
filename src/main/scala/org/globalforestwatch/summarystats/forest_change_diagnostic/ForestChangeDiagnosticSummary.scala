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

        // pixel Area
        val lat: Double = raster.rasterExtent.gridRowToMap(row)
        val area: Double = Geodesy.pixelArea(lat, raster.cellSize) // uses Pixel's center coordiate.  +- raster.cellSize.height/2 doesn't make much of a difference
        val areaHa = area / 10000.0

        // input layers
        val tcd2000: Integer = raster.tile.tcd2000.getData(col, row)
        val lossYear: Int = {
          val loss = raster.tile.loss.getData(col, row)
          if (loss != null) {
            loss.toInt
          } else { 0 }
        }

        val isPrimaryForest: Boolean =
          raster.tile.isPrimaryForest.getData(col, row)
        val isPeatlands: Boolean = raster.tile.isPeatlands.getData(col, row)
        val isIntactForestLandscapes2016: Boolean =
          raster.tile.isIntactForestLandscapes2016.getData(col, row)
        val wdpa: String = raster.tile.wdpaProtectedAreas.getData(col, row)
        val prodesLossYear: Int = raster.tile.prodesLossYear.getData(col, row)
        val seAsiaLandCover: String =
          raster.tile.seAsiaLandCover.getData(col, row)
        val idnLandCover: String = raster.tile.idnLandCover.getData(col, row)
        val isSoyPlantedAreas: Boolean = raster.tile.isSoyPlantedArea.getData(col, row)
        val idnForestArea: String = raster.tile.idnForestArea.getData(col, row)
        val isIDNForestMoratorium: Boolean = raster.tile.isIDNForestMoratorium.getData(col, row)
        val braBiomes: String = raster.tile.braBiomes.getData(col, row)

        // compute Booleans
        val isTreeCoverExtent: Boolean = tcd2000 > 30
        val isLossYear: Boolean = isTreeCoverExtent && lossYear > 0
        val isProtectedAreas: Boolean = wdpa == "Category Ia/b or II"
        val isProdesLoss: Boolean = prodesLossYear > 0

        // summary statistics
        val treeCoverLossTotalYearly = ForestChangeDiagnosticDataLossYearly.fill(lossYear, areaHa, isLossYear)
        val treeCoverLossPrimaryForestYearly = ForestChangeDiagnosticDataLossYearly.fill(lossYear, areaHa, isPrimaryForest && isLossYear)
        val treeCoverLossPeatlandYearly = ForestChangeDiagnosticDataLossYearly.fill(lossYear, areaHa, isPeatlands && isLossYear)
        val treeCoverLossIntactForestYearly = ForestChangeDiagnosticDataLossYearly.fill(lossYear, areaHa, isIntactForestLandscapes2016 && isLossYear)
        val treeCoverLossProtectedAreasYearly = ForestChangeDiagnosticDataLossYearly.fill(lossYear, areaHa, isProtectedAreas && isLossYear)
        val treeCoverLossSEAsiaLandCoverYearly =
          ForestChangeDiagnosticDataLossYearlyCategory.fill(seAsiaLandCover, lossYear, areaHa, "Unknown", isLossYear)
        val treeCoverLossIDNLandCoverYearly =
          ForestChangeDiagnosticDataLossYearlyCategory.fill(idnLandCover, lossYear, areaHa, "", isLossYear)
        val treeCoverLossSoyPlantedAreasYearly = ForestChangeDiagnosticDataLossYearly.fill(lossYear, areaHa, isSoyPlantedAreas && isLossYear)
        val treeCoverLossIDNForestAreaYearly = ForestChangeDiagnosticDataLossYearlyCategory.fill(idnForestArea, lossYear, areaHa, "", isLossYear)
        val treeCoverLossIDNForestMoratoriumYearly = ForestChangeDiagnosticDataLossYearly.fill(lossYear, areaHa, isIDNForestMoratorium && isLossYear)
        val prodesLossYearly = ForestChangeDiagnosticDataLossYearly.fill(prodesLossYear, areaHa, isProdesLoss)
        val prodesLossProtectedAreasYearly = ForestChangeDiagnosticDataLossYearly.fill(prodesLossYear, areaHa, isProdesLoss && isProtectedAreas)
        val prodesLossPrimaryForestYearly = ForestChangeDiagnosticDataLossYearly.fill(prodesLossYear, areaHa, isProdesLoss && isPrimaryForest)
        val treeCoverLossBRABiomesYearly = ForestChangeDiagnosticDataLossYearlyCategory.fill(braBiomes, lossYear, areaHa, "Unknown", isLossYear)
        val treeCoverExtent = ForestChangeDiagnosticDataDouble.fill(areaHa, isTreeCoverExtent)
        val treeCoverExtentPrimaryForest = ForestChangeDiagnosticDataDouble.fill(areaHa, isTreeCoverExtent && isPrimaryForest)
        val treeCoverExtentProtectedAreas = ForestChangeDiagnosticDataDouble.fill(areaHa, isTreeCoverExtent && isProtectedAreas)
        val treeCoverExtentPeatlands = ForestChangeDiagnosticDataDouble.fill(areaHa, isTreeCoverExtent && isPeatlands)
        val treeCoverExtentIntactForests = ForestChangeDiagnosticDataDouble.fill(areaHa, isTreeCoverExtent && isIntactForestLandscapes2016)


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
            treeCoverLossIDNForestMoratoriumYearly,
            prodesLossYearly,
            prodesLossProtectedAreasYearly,
            prodesLossPrimaryForestYearly,
            treeCoverLossBRABiomesYearly,
            treeCoverExtent,
            treeCoverExtentPrimaryForest,
            treeCoverExtentProtectedAreas,
            treeCoverExtentPeatlands,
            treeCoverExtentIntactForests
          )

          acc = ForestChangeDiagnosticSummary(acc.stats.merge(newStats))


      }
    }
}
