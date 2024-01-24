package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.implicits._
import geotrellis.raster._
import geotrellis.raster.summary.GridVisitor
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
    if (stats.isEmpty) {
      ForestChangeDiagnosticData.empty
    } else {
      stats
        .map { case (group, data) => group.toForestChangeDiagnosticData(data.totalArea) }
        .foldLeft(ForestChangeDiagnosticData.empty)(_ merge _)
    }
  }

  def isEmpty = stats.isEmpty
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

        val gfwProCoverage: Map[String, Boolean] =
          raster.tile.gfwProCoverage.getData(col, row)
        val argPresence = gfwProCoverage.getOrElse("Argentina", false)
        val braBiomes: String = raster.tile.braBiomes.getData(col, row)

        // We compute country-specific forest loss using argForestLoss tile for
        // Argentina, and prodesLossYear for Brazil. In the very unusual case where a
        // location covers more than one country, we don't want to mix
        // country-specific forest losses, so we record the country-code that the
        // forest loss came from. We will convert the location to an error if we end
        // up merging results from more than one country.
        val (countrySpecificLossYear: Int, countryCode: String) =
          if (argPresence) {
            val possLoss = raster.tile.argForestLoss.getData(col, row)
            if (possLoss != null && possLoss.toInt > 0) {
              (possLoss.toInt, "ARG")
            } else {
              (0, "ARG")
            }
          } else {
            val possLoss = raster.tile.prodesLossYear.getData(col, row)
            if (possLoss != null && possLoss.toInt > 0) {
              (possLoss.toInt, "BRA")
            } else if (braBiomes != "Not applicable") {
              (0, "BRA")
            } else {
              (0, "")
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
        val isPlantation: Boolean = raster.tile.isPlantation.getData(col, row)
        val argOTBN: String = raster.tile.argOTBN.getData(col, row)

        // compute Booleans
        val isTreeCoverExtent30: Boolean = tcd2000 > 30
        val isTreeCoverExtent90: Boolean = tcd2000 > 90
        val isUMDLoss: Boolean = isTreeCoverExtent30 && umdTreeCoverLossYear > 0
        val isCountrySpecificLoss: Boolean = countrySpecificLossYear > 0

        val southAmericaPresence =
          gfwProCoverage.getOrElse("South America", false)
        val legalAmazonPresence =
          gfwProCoverage.getOrElse("Legal Amazon", false)
        val braBiomesPresence = gfwProCoverage.getOrElse("Brazil Biomes", false)
        val cerradoBiomesPresence =
          gfwProCoverage.getOrElse("Cerrado Biomes", false)
        val seAsiaPresence = gfwProCoverage.getOrElse("South East Asia", false)
        val idnPresence = gfwProCoverage.getOrElse("Indonesia", false)

        val protectedAreaCategory = raster.tile.protectedAreasByCategory.getData(col, row)
        val isProtectedArea = (protectedAreaCategory != "")

        // Currently, only do the area intersection with the detailed WDPA categories
        // if location is in Argentina. Similarly, only do area intersection with
        // Landmark (indigenous territories) if in Argentina.
        // With lazy tile loading, the landmark tiles are only loaded if
        // argPresence is true.
        val detailedWdpa = if (argPresence)
          protectedAreaCategory
        else
          ""
        // We will likely have different Landmark categories for other countries, but
        // there is no distinction currently for Argentina, so we put all of the
        // indigenous area into the "Not Reported" category.
        val landmarkCategory = if (argPresence)
          (if (raster.tile.landmark.getData(col, row)) "Not Reported" else "")
        else
          ""

        val groupKey = ForestChangeDiagnosticRawDataGroup(
          umdTreeCoverLossYear,
          isUMDLoss,
          countryCode,
          countrySpecificLossYear,
          isCountrySpecificLoss,
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
          argOTBN,
          southAmericaPresence,
          legalAmazonPresence,
          braBiomesPresence,
          cerradoBiomesPresence,
          seAsiaPresence,
          idnPresence,
          argPresence,
          detailedWdpa,
          landmarkCategory,
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
