package org.globalforestwatch.summarystats.carbon_sensitivity

import cats.implicits._
import geotrellis.raster._
import geotrellis.raster.summary.GridVisitor
import org.globalforestwatch.summarystats.Summary
import org.globalforestwatch.util.Geodesy
import org.globalforestwatch.util.Implicits._
import org.globalforestwatch.util.Util.getAnyMapValue

import scala.annotation.tailrec

/** LossData Summary by year */
case class CarbonSensitivitySummary(
                                     stats: Map[CarbonSensitivityDataGroup, CarbonSensitivityData] = Map.empty
                                   ) extends Summary[CarbonSensitivitySummary] {

  /** Combine two Maps and combine their LossData when a year is present in both */
  def merge(other: CarbonSensitivitySummary): CarbonSensitivitySummary = {
    // the years.combine method uses LossData.lossDataSemigroup instance to perform per value combine on the map
    CarbonSensitivitySummary(stats.combine(other.stats))
  }
}

object CarbonSensitivitySummary {
  // CarbonSensitivitySummary form Raster[CarbonSensitivityTile] -- cell types may not be the same

  def getGridVisitor(kwargs: Map[String, Any]) : GridVisitor[Raster[CarbonSensitivityTile], CarbonSensitivitySummary] = {
    new GridVisitor[Raster[CarbonSensitivityTile], CarbonSensitivitySummary] {
      private var acc: CarbonSensitivitySummary = new CarbonSensitivitySummary()

      def result: CarbonSensitivitySummary = acc

      def visit(
                    raster: Raster[CarbonSensitivityTile],
                    col: Int,
                    row: Int
                  ): Unit = {

        // Changes the lossYear type to PRODES if the sensitivity analysis is "legal_Amazon_loss"
        val changeLossSource: String =
          getAnyMapValue[String](kwargs, "sensitivityType")

        // This is a pixel by pixel operation
        val lossYear: Integer = {
          if (changeLossSource == "legal_Amazon_loss") raster.tile.lossLegalAmazon.getData(col, row)
          else raster.tile.loss.getData(col, row)
        }
        val tcd2000: Integer = raster.tile.tcd2000.getData(col, row)
        val biomass: Double = raster.tile.biomass.getData(col, row)
        val grossCumulAbovegroundRemovalsCo2: Float = raster.tile.grossCumulAbovegroundRemovalsCo2.getData(col, row)
        val grossCumulBelowgroundRemovalsCo2: Float = raster.tile.grossCumulBelowgroundRemovalsCo2.getData(col, row)
        val netFluxCo2: Float = raster.tile.netFluxCo2.getData(col, row)
        val agcEmisYear: Float = raster.tile.agcEmisYear.getData(col, row)
        val soilCarbonEmisYear: Float = raster.tile.soilCarbonEmisYear.getData(col, row)
        val grossEmissionsCo2eNonCo2: Float = raster.tile.grossEmissionsCo2eNonCo2.getData(col, row)
        val grossEmissionsCo2eCo2Only: Float = raster.tile.grossEmissionsCo2eCo2Only.getData(col, row)
        val jplTropicsAbovegroundBiomassDensity2000: Float = raster.tile.jplTropicsAbovegroundBiomassDensity2000.getData(col, row)

        val isGain: Boolean = raster.tile.gain.getData(col, row)
        val fluxModelExtent: Boolean = raster.tile.fluxModelExtent.getData(col, row)
        val removalForestType: String = raster.tile.removalForestType.getData(col, row)
        val mangroveBiomassExtent: Boolean = raster.tile.mangroveBiomassExtent.getData(col, row)
        val drivers: String = raster.tile.drivers.getData(col, row)
        val wdpa: String = raster.tile.wdpa.getData(col, row)
        val plantationsTypeFluxModel: String = raster.tile.plantationsTypeFluxModel.getData(col, row)
        val ecozones: String = raster.tile.ecozones.getData(col, row)
        val intactForestLandscapes: String = raster.tile.intactForestLandscapes.getData(col, row)
        val landRights: Boolean = raster.tile.landRights.getData(col, row)
        val intactPrimaryForest: Boolean = raster.tile.intactPrimaryForest.getData(col, row)
        val peatlandsExtentFluxModel: Boolean = raster.tile.peatlandsExtentFluxModel.getData(col, row)
        val forestAgeCategory: String = raster.tile.forestAgeCategory.getData(col, row)
        val jplTropicsAbovegroundBiomassExtent2000: Boolean = raster.tile.jplTropicsAbovegroundBiomassExtent2000.getData(col, row)
        val fiaRegionsUsExtent: String = raster.tile.fiaRegionsUsExtent.getData(col, row)
        val braBiomes: String = raster.tile.braBiomes.getData(col, row)
        val riverBasins: String = raster.tile.riverBasins.getData(col, row)
        val primaryForest: Boolean = raster.tile.primaryForest.getData(col, row)
        val lossYearLegalAmazon: Integer = raster.tile.lossLegalAmazon.getData(col, row)
        val prodesLegalAmazonExtent2000: Boolean = raster.tile.prodesLegalAmazonExtent2000.getData(col, row)
        val tropicLatitudeExtent: Boolean = raster.tile.tropicLatitudeExtent.getData(col, row)
        val burnYearHansenLoss: Integer = raster.tile.burnYearHansenLoss.getData(col, row)
        val grossEmissionsNodeCodes: String = raster.tile.grossEmissionsNodeCodes.getData(col, row)

        val lat: Double = raster.rasterExtent.gridRowToMap(row)
        val area: Double = Geodesy.pixelArea(lat, raster.cellSize) // uses Pixel's center coordinate.  +- raster.cellSize.height/2 doesn't make much of a difference

        val areaHa = area / 10000.0

        val carbonfluxLossYear: Integer = if (lossYear != null) lossYear else null
        val isLoss: Boolean = carbonfluxLossYear != null

        val carbonfluxLossYearLegalAmazon: Integer = if (lossYearLegalAmazon != null
          && lossYearLegalAmazon >= 2001) lossYearLegalAmazon else null
        val isLossLegalAmazon: Boolean = carbonfluxLossYearLegalAmazon != null

        // Creates variable of whether the Hansen loss coincided with burning
        val isBurnLoss: Boolean = burnYearHansenLoss != null

        // Calculates model extent area. Need to convert from boolean to integer, unlike in
        // annualupdate_minimal package where gain bollean can be multiplied by areaHa directly. Not sure why different
        // here.
        val fluxModelExtentAreaInt: Integer = if (fluxModelExtent) 1 else 0
        val fluxModelExtentAreaPixel: Double = fluxModelExtentAreaInt * areaHa

        val biomassPixel = biomass * areaHa
        val grossCumulAbovegroundRemovalsCo2Pixel = grossCumulAbovegroundRemovalsCo2 * areaHa
        val grossCumulBelowgroundRemovalsCo2Pixel = grossCumulBelowgroundRemovalsCo2 * areaHa
        val grossCumulAboveBelowgroundRemovalsCo2Pixel = grossCumulAbovegroundRemovalsCo2Pixel + grossCumulBelowgroundRemovalsCo2Pixel
        val netFluxCo2Pixel = netFluxCo2 * areaHa
        val agcEmisYearPixel = agcEmisYear * areaHa
        val soilCarbonEmisYearPixel = soilCarbonEmisYear * areaHa
        val grossEmissionsCo2eNonCo2Pixel = grossEmissionsCo2eNonCo2 * areaHa
        val grossEmissionsCo2eCo2OnlyPixel = grossEmissionsCo2eCo2Only * areaHa
        val grossEmissionsCo2e = grossEmissionsCo2eNonCo2 + grossEmissionsCo2eCo2Only
        val grossEmissionsCo2ePixel = grossEmissionsCo2e * areaHa
        val jplTropicsAbovegroundBiomassDensity2000Pixel = jplTropicsAbovegroundBiomassDensity2000 * areaHa

        val thresholds = List(0, 10, 15, 20, 25, 30, 50, 75)


        @tailrec
        def updateSummary(
                           thresholds: List[Int],
                           stats: Map[CarbonSensitivityDataGroup, CarbonSensitivityData]
                         ): Map[CarbonSensitivityDataGroup, CarbonSensitivityData] = {
          if (thresholds == Nil) stats
          else {
            val pKey = CarbonSensitivityDataGroup(
              fluxModelExtent,
              removalForestType,
              lossYear,
              thresholds.head,
              isGain,
              isLoss,
              mangroveBiomassExtent,
              drivers,
              ecozones,
              landRights,
              wdpa,
              intactForestLandscapes,
              plantationsTypeFluxModel,
              intactPrimaryForest,
              peatlandsExtentFluxModel,
              forestAgeCategory,
              jplTropicsAbovegroundBiomassExtent2000,
              fiaRegionsUsExtent,
              braBiomes,
              riverBasins,
              primaryForest,
              isLossLegalAmazon,
              prodesLegalAmazonExtent2000,
              tropicLatitudeExtent,
              isBurnLoss,
              grossEmissionsNodeCodes
            )

            val summary: CarbonSensitivityData =
              stats.getOrElse(
                key = pKey,
                default = CarbonSensitivityData(
                  0, 0, 0, 0, 0, 0,
                  0, 0, 0, 0, 0, 0,
                  0, 0, 0, 0, 0)

              )

            summary.totalArea += areaHa

            if (tcd2000 >= thresholds.head) {

              if (carbonfluxLossYear != null) {
                summary.totalTreecoverLoss += areaHa
                summary.totalBiomassLoss += biomassPixel
                summary.totalGrossEmissionsCo2eCo2Only += grossEmissionsCo2eCo2OnlyPixel
                summary.totalGrossEmissionsCo2eNonCo2 += grossEmissionsCo2eNonCo2Pixel
                summary.totalGrossEmissionsCo2e += grossEmissionsCo2ePixel
                summary.totalAgcEmisYear += agcEmisYearPixel
                summary.totalSoilCarbonEmisYear += soilCarbonEmisYearPixel
              }

              if (isLossLegalAmazon) summary.totalTreecoverLossLegalAmazon += areaHa

              summary.totalTreecoverExtent2000 += areaHa
              summary.totalBiomass += biomassPixel
              summary.totalGrossCumulAbovegroundRemovalsCo2 += grossCumulAbovegroundRemovalsCo2Pixel
              summary.totalGrossCumulBelowgroundRemovalsCo2 += grossCumulBelowgroundRemovalsCo2Pixel
              summary.totalGrossCumulAboveBelowgroundRemovalsCo2 += grossCumulAboveBelowgroundRemovalsCo2Pixel
              summary.totalNetFluxCo2 += netFluxCo2Pixel
              summary.totalJplTropicsAbovegroundBiomassDensity2000 += jplTropicsAbovegroundBiomassDensity2000Pixel

              summary.totalFluxModelExtentArea += fluxModelExtentAreaPixel
            }

            updateSummary(thresholds.tail, stats.updated(pKey, summary))
          }
        }

        val updatedSummary: Map[CarbonSensitivityDataGroup, CarbonSensitivityData] =
          updateSummary(thresholds, acc.stats)

        acc = CarbonSensitivitySummary(updatedSummary)
      }
    }
  }
}