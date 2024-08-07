package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.implicits._
import geotrellis.raster._
import geotrellis.raster.summary.GridVisitor
import org.globalforestwatch.summarystats.Summary
import org.globalforestwatch.util.Geodesy
import org.globalforestwatch.layers.ApproxYear
import org.globalforestwatch.layers.GFWProCoverage

/** ForestChangeDiagnosticRawData broken down by ForestChangeDiagnosticRawDataGroup,
  * which includes the loss year, but lots of other characteristics as well. */
case class ForestChangeDiagnosticSummary(
                                          stats: Map[ForestChangeDiagnosticRawDataGroup,
                                            ForestChangeDiagnosticRawData] = Map.empty
                                        ) extends Summary[ForestChangeDiagnosticSummary] {

  /** Combine two Maps by combining ForestChangeDiagnosticRawDataGroup entries that
    * have the same values. This merge function is used by
    * summaryStats.summarySemigroup to define a combine operation on
    * ForestChangeDiagnosticSummary, which is used to combine records with the same
    * FeatureId in ErrorSummaryRDD. */
  def merge(
    other: ForestChangeDiagnosticSummary
  ): ForestChangeDiagnosticSummary = {
    // the stats.combine method uses the
    // ForestChangeDiagnosticRawData.lossDataSemigroup instance to perform per-value
    // combine on the map.
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
  // Cell types of Raster[ForestChangeDiagnosticTile] may not be the same.

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
        val re: RasterExtent = raster.rasterExtent
        val lat: Double = re.gridRowToMap(row)
        // uses Pixel's center coordinate. +/- raster.cellSize.height/2
        // doesn't make much of a difference
        val area: Double = Geodesy.pixelArea(lat, re.cellSize)
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

        val region: Int = raster.tile.gfwProCoverage.getData(col, row)
        val argPresence = GFWProCoverage.isArgentina(region)
        val colPresence = GFWProCoverage.isColombia(region)
        val braBiomesPresence = GFWProCoverage.isBrazilBiomesPresence(region)
        var argOTBN: String = ""

        // We compute country-specific forest loss using argForestLoss tile for
        // Argentina, and prodesLossYear for Brazil. In the very unusual case where a
        // location covers more than one country, we don't want to mix
        // country-specific forest losses, so we record the country-code that the
        // forest loss came from.
        var countrySpecificLossYear = ApproxYear(0, false)
        var countryCode  = ""
        var classifiedRegion = ""

        if (colPresence) {
          countryCode = "COL"
          classifiedRegion = raster.tile.colFronteraAgricola.getData(col, row)
        } else if (argPresence) {
          countrySpecificLossYear = raster.tile.argForestLoss.getData(col, row)
          argOTBN = raster.tile.argOTBN.getData(col, row)
          classifiedRegion = argOTBN
          countryCode = "ARG"
        } else {
          val possLoss = raster.tile.prodesLossYear.getData(col, row)
          if (possLoss != null && possLoss > 0) {
            countrySpecificLossYear = ApproxYear(possLoss, false)
            countryCode = "BRA"
          } else if (braBiomesPresence) {
            // braBiomesPresence is all of Brazil, except for a few estuaries and
            // other areas along the coastline, so we are using it as a proxy for
            // pixel being in Brazil. We are also in Brazil if there was prodes loss.
            // (We will aim to have a true gfwProCoverage value for Brazil presence soon.)
            countryCode = "BRA"
          }
        }
        val prodesLossYear: Int = {
          if (countryCode == "BRA") {
            countrySpecificLossYear.year
          } else {
            0
          }
        }
        val braBiomes: String = {
          if (braBiomesPresence) {
            raster.tile.braBiomes.getData(col, row)
          } else {
            ""
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

        // compute Booleans
        val isTreeCoverExtent30: Boolean = tcd2000 > 30
        val isTreeCoverExtent90: Boolean = tcd2000 > 90
        val isUMDLoss: Boolean = isTreeCoverExtent30 && umdTreeCoverLossYear > 0
        val isProdesLoss: Boolean = prodesLossYear > 0
        val isCountrySpecificLoss: Boolean = countrySpecificLossYear.year > 0

        val southAmericaPresence = GFWProCoverage.isSouthAmerica(region)
        val legalAmazonPresence = GFWProCoverage.isLegalAmazonPresence(region)
        val cerradoBiomesPresence = GFWProCoverage.isCerradoBiomesPresence(region)
        val seAsiaPresence = GFWProCoverage.isSouthEastAsia(region)
        val idnPresence = GFWProCoverage.isIndonesia(region)

        val protectedAreaCategory = raster.tile.protectedAreasByCategory.getData(col, row)
        val isProtectedArea = (protectedAreaCategory != "")

        // Currently, only do the area intersection with the detailed WDPA categories
        // if location is in ARG or COL. Similarly, only do area intersection with
        // Landmark (indigenous territories) if in ARG or COL.
        // With lazy tile loading, the landmark tiles are only loaded if
        // argPresence or colPresence is true.
        val detailedWdpa = if (argPresence || colPresence)
          protectedAreaCategory
        else
          ""
        // We will likely have different Landmark categories for other countries, but
        // there is no distinction currently for Argentina, so we put all of the
        // indigenous area into the "Not Reported" category.
        val landmarkCategory = if (argPresence || colPresence)
          (if (raster.tile.landmark.getData(col, row)) "Not Reported" else "")
        else
          ""

        val groupKey = ForestChangeDiagnosticRawDataGroup(
          umdTreeCoverLossYear,
          isUMDLoss,
          prodesLossYear,
          isProdesLoss,
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
          classifiedRegion,
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
