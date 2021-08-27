package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.implicits._
import geotrellis.raster._
import geotrellis.raster.summary.GridVisitor
import org.globalforestwatch.features.FeatureId
import org.globalforestwatch.summarystats.Summary
import org.globalforestwatch.util.Geodesy

/** LossData Summary by year */
case class ForestChangeDiagnosticSummary(
                                          stats: Map[ForestChangeDiagnosticRawDataGroup,
                                            ForestChangeDiagnosticRawData] = Map.empty
                                        ) extends Summary[ForestChangeDiagnosticSummary] {

  /** Combine two Maps and combine their LossData when a year is present in both */
  def merge(
    other: ForestChangeDiagnosticSummary
  ): ForestChangeDiagnosticSummary = {
    // the years.combine method uses LossData.lossDataSemigroup instance to perform per value combine on the map
    ForestChangeDiagnosticSummary(stats.combine(other.stats))
  }

  /** Pivot raw data to ForestChangeDiagnosticData and aggregate across years */
  def toForestChangeDiagnosticData(): ForestChangeDiagnosticData = {
    stats
      .map { case (group, data) => group.toForestChangeDiagnosticData(data.totalArea) }
      .foldLeft(ForestChangeDiagnosticData.empty)( _ merge _)
  }
}

object ForestChangeDiagnosticSummary {
  // ForestChangeDiagnosticSummary from Raster[ForestChangeDiagnosticTile] -- cell types may not be the same

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

        val umdTreeCoverLossYear: Int = {
          val loss = raster.tile.loss.getData(col, row)
          if (loss != null) {
            loss.toInt
          } else {
            0
          }
        }

        val isPrimaryForest: Boolean =
          raster.tile.isPrimaryForest.getData(col, row)
        val isPeatlands: Boolean = raster.tile.isPeatlands.getData(col, row)
        val isIntactForestLandscapes2000: Boolean =
          raster.tile.isIntactForestLandscapes2000.getData(col, row)
        val wdpa: String = raster.tile.wdpaProtectedAreas.getData(col, row)
        val prodesLossYear: Int = {
          val loss = raster.tile.prodesLossYear.getData(col, row)
          if (loss != null) {
            loss.toInt
          } else {
            0
          }
        }
        val seAsiaLandCover: String =
          raster.tile.seAsiaLandCover.getData(col, row)
        val idnLandCover: String = raster.tile.idnLandCover.getData(col, row)
        val isSoyPlantedAreas: Boolean =
          raster.tile.isSoyPlantedArea.getData(col, row)
        val idnForestArea: String = raster.tile.idnForestArea.getData(col, row)
        val isIdnForestMoratorium: Boolean =
          raster.tile.isIDNForestMoratorium.getData(col, row)
        val braBiomes: String = raster.tile.braBiomes.getData(col, row)
        val isPlantation: Boolean = raster.tile.isPlantation.getData(col, row)
        val gfwProCoverage: Map[String, Boolean] =
          raster.tile.gfwProCoverage.getData(col, row)

        // compute Booleans
        val isTreeCoverExtent30: Boolean = tcd2000 > 30
        val isTreeCoverExtent90: Boolean = tcd2000 > 90
        val isUMDLoss: Boolean = isTreeCoverExtent30 && umdTreeCoverLossYear > 0
        val isProtectedArea: Boolean = wdpa != ""
        val isProdesLoss: Boolean = prodesLossYear > 0

        val southAmericaPresence =
          gfwProCoverage.getOrElse("South America", false)
        val legalAmazonPresence =
          gfwProCoverage.getOrElse("Legal Amazon", false)
        val braBiomesPresence = gfwProCoverage.getOrElse("Brazil Biomes", false)
        val cerradoBiomesPresence =
          gfwProCoverage.getOrElse("Cerrado Biomes", false)
        val seAsiaPresence = gfwProCoverage.getOrElse("South East Asia", false)
        val idnPresence = gfwProCoverage.getOrElse("Indonesia", false)

        val groupKey = ForestChangeDiagnosticRawDataGroup(
          umdTreeCoverLossYear,
          isUMDLoss,
          prodesLossYear,
          isProdesLoss,
          isTreeCoverExtent30,
          isTreeCoverExtent90,
          isPrimaryForest,
          isPeatlands,
          isIntactForestLandscapes2000,
          isProtectedArea,
          seAsiaLandCover,
          idnLandCover,
          isSoyPlantedAreas,
          idnForestArea,
          isIdnForestMoratorium,
          braBiomes,
          isPlantation,
          southAmericaPresence,
          legalAmazonPresence,
          braBiomesPresence,
          cerradoBiomesPresence,
          seAsiaPresence,
          idnPresence
        )

        val summaryData: ForestChangeDiagnosticRawData =
          acc.stats.getOrElse(
            key = groupKey,
            default = ForestChangeDiagnosticRawData(0)
          )

        summaryData.totalArea += areaHa

        val new_stats = acc.stats.updated(groupKey, summaryData)
        acc = ForestChangeDiagnosticSummary(new_stats)

      }
    }
}
