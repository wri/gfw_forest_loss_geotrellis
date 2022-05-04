package org.globalforestwatch.summarystats.carbonflux

import cats.implicits._
import geotrellis.raster.summary.GridVisitor
import geotrellis.raster._
import org.globalforestwatch.summarystats.Summary
import org.globalforestwatch.util.Geodesy

import scala.annotation.tailrec

/** LossData Summary by year */
case class CarbonFluxSummary(
                              stats: Map[CarbonFluxDataGroup, CarbonFluxData] = Map.empty
                            ) extends Summary[CarbonFluxSummary] {

  /** Combine two Maps and combine their LossData when a year is present in both */
  def merge(other: CarbonFluxSummary): CarbonFluxSummary = {
    // the years.combine method uses LossData.lossDataSemigroup instance to perform per value combine on the map
    CarbonFluxSummary(stats.combine(other.stats))
  }
  def isEmpty = stats.isEmpty
}

object CarbonFluxSummary {
  // CarbonFluxSummary form Raster[CarbonFluxTile] -- cell types may not be the same

  def getGridVisitor(kwargs: Map[String, Any]) : GridVisitor[Raster[CarbonFluxTile], CarbonFluxSummary] = {
      new GridVisitor[Raster[CarbonFluxTile], CarbonFluxSummary] {
        private var acc: CarbonFluxSummary = new CarbonFluxSummary()

        def result: CarbonFluxSummary = acc

        def visit(raster: Raster[CarbonFluxTile],
                    col: Int,
                    row: Int): Unit = {
        // This is a pixel by pixel operation
        val lossYear: Integer = raster.tile.loss.getData(col, row)
        val tcd2000: Integer = raster.tile.tcd2000.getData(col, row)
        val biomass: Double = raster.tile.biomass.getData(col, row)

        val grossAnnualAbovegroundRemovalsCarbon: Float = raster.tile.grossAnnualAbovegroundRemovalsCarbon.getData(col, row)
        val grossAnnualBelowgroundRemovalsCarbon: Float = raster.tile.grossAnnualBelowgroundRemovalsCarbon.getData(col, row)
        val grossCumulAbovegroundRemovalsCo2: Float = raster.tile.grossCumulAbovegroundRemovalsCo2.getData(col, row)
        val grossCumulBelowgroundRemovalsCo2: Float = raster.tile.grossCumulBelowgroundRemovalsCo2.getData(col, row)
        val netFluxCo2: Float = raster.tile.netFluxCo2.getData(col, row)
        val agcEmisYear: Float = raster.tile.agcEmisYear.getData(col, row)
        val bgcEmisYear: Float = raster.tile.bgcEmisYear.getData(col, row)
        val deadwoodCarbonEmisYear: Float = raster.tile.deadwoodCarbonEmisYear.getData(col, row)
        val litterCarbonEmisYear: Float = raster.tile.litterCarbonEmisYear.getData(col, row)
        val soilCarbonEmisYear: Float = raster.tile.soilCarbonEmisYear.getData(col, row)
        val agc2000: Float = raster.tile.agc2000.getData(col, row)
        val bgc2000: Float = raster.tile.bgc2000.getData(col, row)
        val deadwoodCarbon2000: Float = raster.tile.deadwoodCarbon2000.getData(col, row)
        val litterCarbon2000: Float = raster.tile.litterCarbon2000.getData(col, row)
        val soilCarbon2000: Float = raster.tile.soilCarbon2000.getData(col, row)
        val grossEmissionsCo2eNonCo2BiomassSoil: Float = raster.tile.grossEmissionsCo2eNonCo2BiomassSoil.getData(col, row)
        val grossEmissionsCo2eCo2OnlyBiomassSoil: Float =  raster.tile.grossEmissionsCo2eCo2OnlyBiomassSoil.getData(col, row)
        val grossEmissionsCo2eNonCo2SoilOnly: Float = raster.tile.grossEmissionsCo2eNonCo2SoilOnly.getData(col, row)
        val grossEmissionsCo2eCo2OnlySoilOnly: Float =  raster.tile.grossEmissionsCo2eCo2OnlySoilOnly.getData(col, row)
        val jplTropicsAbovegroundBiomassDensity2000: Float = raster.tile.jplTropicsAbovegroundBiomassDensity2000.getData(col, row)
        val stdevAnnualAbovegroundRemovalsCarbon: Float = raster.tile.stdevAnnualAbovegroundRemovalsCarbon.getData(col, row)
        val stdevSoilCarbon2000: Float = raster.tile.stdevSoilCarbon2000.getData(col, row)

        val isGain: Boolean = raster.tile.gain.getData(col, row)
        val fluxModelExtent: Boolean = raster.tile.fluxModelExtent.getData(col, row)
        val removalForestType: String = raster.tile.removalForestType.getData(col, row)
        val mangroveBiomassExtent: Boolean = raster.tile.mangroveBiomassExtent.getData(col, row)
        val drivers: String = raster.tile.drivers.getData(col, row)
        val wdpa: String = raster.tile.wdpa.getData(col, row)
        val plantationsTypeFluxModel: String = raster.tile.plantationsTypeFluxModel.getData(col, row)
        val faoEcozones2000: String = raster.tile.faoEcozones2000.getData(col, row)
        val intactForestLandscapes: String = raster.tile.intactForestLandscapes.getData(col, row)
        val landmark: Boolean = raster.tile.landmark.getData(col, row)
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
        val area: Double = Geodesy.pixelArea(lat, raster.cellSize) // uses Pixel's center coordiate.  +- raster.cellSize.height/2 doesn't make much of a difference

        val areaHa = area / 10000.0

//      val carbonfluxLossYear: Integer = if (lossYear != null && lossYear >= 2001 && lossYear <= 2015) lossYear else null
        val isLoss: Boolean = lossYear != null

//        val carbonfluxLossYearLegalAmazon: Integer = if (lossYearLegalAmazon != null
//          && lossYearLegalAmazon >= 2001 && lossYearLegalAmazon <= 2015) lossYearLegalAmazon else null
        val isLossLegalAmazon: Boolean = lossYearLegalAmazon != null

        // Creates variable of whether the Hansen loss coincided with burning
        val isBurnLoss: Boolean = burnYearHansenLoss != null

        // Calculates model extent area. Need to convert from boolean to integer, unlike in
        // annualupdate_minimal package where gain bollean can be multiplied by areaHa directly. Not sure why different
        // here.
        val fluxModelExtentAreaInt: Integer = if (fluxModelExtent) 1 else 0
        val fluxModelExtentAreaPixel: Double = fluxModelExtentAreaInt * areaHa

        val biomassPixel = biomass * areaHa
        val grossAnnualAbovegroundRemovalsCarbonPixel = grossAnnualAbovegroundRemovalsCarbon * areaHa
        val grossAnnualBelowgroundRemovalsCarbonPixel = grossAnnualBelowgroundRemovalsCarbon * areaHa
        val grossCumulAbovegroundRemovalsCo2Pixel = grossCumulAbovegroundRemovalsCo2 * areaHa
        val grossCumulBelowgroundRemovalsCo2Pixel = grossCumulBelowgroundRemovalsCo2 * areaHa
        val grossAnnualAboveBelowgroundRemovalsCarbonPixel = grossAnnualAbovegroundRemovalsCarbonPixel + grossAnnualBelowgroundRemovalsCarbonPixel
        val grossCumulAboveBelowgroundRemovalsCo2Pixel = grossCumulAbovegroundRemovalsCo2Pixel + grossCumulBelowgroundRemovalsCo2Pixel

        val netFluxCo2Pixel = netFluxCo2 * areaHa
        val agcEmisYearPixel = agcEmisYear * areaHa
        val bgcEmisYearPixel = bgcEmisYear * areaHa
        val deadwoodCarbonEmisYearPixel = deadwoodCarbonEmisYear * areaHa
        val litterCarbonEmisYearPixel = litterCarbonEmisYear * areaHa
        val soilCarbonEmisYearPixel = soilCarbonEmisYear * areaHa
        val totalCarbonEmisYear = agcEmisYear + bgcEmisYear + deadwoodCarbonEmisYear + litterCarbonEmisYear + soilCarbonEmisYear
        val totalCarbonEmisYearPixel = totalCarbonEmisYear * areaHa
        val agc2000Pixel = agc2000 * areaHa
        val bgc2000Pixel = bgc2000 * areaHa
        val deadwoodCarbon2000Pixel = deadwoodCarbon2000 * areaHa
        val litterCarbon2000Pixel = litterCarbon2000 * areaHa
        val soilCarbon2000Pixel = soilCarbon2000 * areaHa
        val totalCarbon2000 = agc2000 + bgc2000 + deadwoodCarbon2000 + litterCarbon2000 + soilCarbon2000
        val totalCarbon2000Pixel = totalCarbon2000 * areaHa

        val grossEmissionsCo2eNonCo2BiomassSoilPixel = grossEmissionsCo2eNonCo2BiomassSoil * areaHa
        val grossEmissionsCo2eCo2OnlyBiomassSoilPixel = grossEmissionsCo2eCo2OnlyBiomassSoil * areaHa
        val grossEmissionsCo2eBiomassSoil = grossEmissionsCo2eNonCo2BiomassSoil + grossEmissionsCo2eCo2OnlyBiomassSoil
        val grossEmissionsCo2eBiomassSoilPixel = grossEmissionsCo2eBiomassSoil * areaHa

        val grossEmissionsCo2eNonCo2SoilOnlyPixel = grossEmissionsCo2eNonCo2SoilOnly * areaHa
        val grossEmissionsCo2eCo2OnlySoilOnlyPixel = grossEmissionsCo2eCo2OnlySoilOnly * areaHa
        val grossEmissionsCo2eSoilOnly = grossEmissionsCo2eNonCo2SoilOnly + grossEmissionsCo2eCo2OnlySoilOnly
        val grossEmissionsCo2eSoilOnlyPixel = grossEmissionsCo2eSoilOnly * areaHa

        val jplTropicsAbovegroundBiomassDensity2000Pixel = jplTropicsAbovegroundBiomassDensity2000 * areaHa

        // Calculates the variance for each removal factor pixel (units are Mg^2/ha^2/yr^2)
        val varianceAnnualAbovegroundRemovalsCarbonPixel = math.pow(stdevAnnualAbovegroundRemovalsCarbon, 2)

        // Keeps track of the number of pixels with variance in them
        val varianceAnnualAbovegroundRemovalsCarbonCount = if (stdevAnnualAbovegroundRemovalsCarbon != 0) 1 else 0

        // Calculates the variance for each soil carbon pixel (units are Mg^2/ha^2)
        val stdevSoilCarbonEmisYear: Float = if (lossYear != null) stdevSoilCarbon2000 else 0
        val varianceSoilCarbonEmisYearPixel: Double = math.pow(stdevSoilCarbonEmisYear, 2)

        // Keeps track of the number of pixels with variance in them
        val varianceSoilCarbonEmisYearCount = if (lossYear != null && stdevSoilCarbon2000 != 0) 1 else 0

        val thresholds = List(0, 10, 15, 20, 25, 30, 50, 75)

        @tailrec
        def updateSummary(
                           thresholds: List[Int],
                           stats: Map[CarbonFluxDataGroup, CarbonFluxData]
                         ): Map[CarbonFluxDataGroup, CarbonFluxData] = {
          if (thresholds == Nil) stats
          else {
            val pKey = CarbonFluxDataGroup(
              fluxModelExtent,
              removalForestType,
              lossYear,
              thresholds.head,
              isGain,
              isLoss,
              mangroveBiomassExtent,
              drivers,
              faoEcozones2000,
              landmark,
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

            // Number of 0s must match number of summary. items below (including summary.totalArea)
            val summary: CarbonFluxData =
              stats.getOrElse(
                key = pKey,
                default = CarbonFluxData(
                  0, 0, 0, 0, 0, 0,
                  0, 0, 0, 0, 0, 0,
                  0, 0, 0, 0, 0, 0,
                  0, 0, 0, 0, 0, 0,
                  0, 0, 0, 0, 0, 0,
                  0, 0, 0, 0, 0, 0,
                  0)
              )

              summary.totalArea += areaHa

              if (tcd2000 >= thresholds.head) {

                if (lossYear != null) {
                  summary.totalTreecoverLoss += areaHa
                  summary.totalBiomassLoss += biomassPixel
                  summary.totalGrossEmissionsCo2eCo2OnlyBiomassSoil += grossEmissionsCo2eCo2OnlyBiomassSoilPixel
                  summary.totalGrossEmissionsCo2eNonCo2BiomassSoil += grossEmissionsCo2eNonCo2BiomassSoilPixel
                  summary.totalGrossEmissionsCo2eBiomassSoil += grossEmissionsCo2eBiomassSoilPixel
                  summary.totalGrossEmissionsCo2eCo2OnlySoilOnly += grossEmissionsCo2eCo2OnlySoilOnlyPixel
                  summary.totalGrossEmissionsCo2eNonCo2SoilOnly += grossEmissionsCo2eNonCo2SoilOnlyPixel
                  summary.totalGrossEmissionsCo2eSoilOnly += grossEmissionsCo2eSoilOnlyPixel
                  summary.totalAgcEmisYear += agcEmisYearPixel
                  summary.totalBgcEmisYear += bgcEmisYearPixel
                  summary.totalDeadwoodCarbonEmisYear += deadwoodCarbonEmisYearPixel
                  summary.totalLitterCarbonEmisYear += litterCarbonEmisYearPixel
                  summary.totalSoilCarbonEmisYear += soilCarbonEmisYearPixel
                  summary.totalCarbonEmisYear += totalCarbonEmisYearPixel
                  // Reports gross removals within tree cover loss pixels by loss year
                  summary.totalGrossCumulAboveBelowgroundRemovalsCo2 += grossCumulAboveBelowgroundRemovalsCo2Pixel
                }
                if (isLossLegalAmazon) summary.totalTreecoverLossLegalAmazon += areaHa

                summary.totalTreecoverExtent2000 += areaHa
                summary.totalBiomass += biomassPixel

                summary.totalGrossAnnualAbovegroundRemovalsCarbon += grossAnnualAbovegroundRemovalsCarbonPixel
                summary.totalGrossAnnualBelowgroundRemovalsCarbon += grossAnnualBelowgroundRemovalsCarbonPixel
                summary.totalGrossAnnualAboveBelowgroundRemovalsCarbon += grossAnnualAboveBelowgroundRemovalsCarbonPixel
                summary.totalGrossCumulAbovegroundRemovalsCo2 += grossCumulAbovegroundRemovalsCo2Pixel
                summary.totalGrossCumulBelowgroundRemovalsCo2 += grossCumulBelowgroundRemovalsCo2Pixel

                // Reports gross removals outside tree cover loss pixels.
                // These two lines of gross removals (inside and outside TCL) are the total gross removals
                if (lossYear == null) {
                  summary.totalGrossCumulAboveBelowgroundRemovalsCo2 += grossCumulAboveBelowgroundRemovalsCo2Pixel
                }

                summary.totalNetFluxCo2 += netFluxCo2Pixel
                summary.totalAgc2000 += agc2000Pixel
                summary.totalBgc2000 += bgc2000Pixel
                summary.totalDeadwoodCarbon2000 += deadwoodCarbon2000Pixel
                summary.totalLitterCarbon2000 += litterCarbon2000Pixel
                summary.totalSoilCarbon2000 += soilCarbon2000Pixel
                summary.totalCarbon2000 += totalCarbon2000Pixel
                summary.totalJplTropicsAbovegroundBiomassDensity2000 += jplTropicsAbovegroundBiomassDensity2000Pixel

                summary.totalVarianceAnnualAbovegroundRemovalsCarbon += varianceAnnualAbovegroundRemovalsCarbonPixel
                summary.totalVarianceAnnualAbovegroundRemovalsCarbonCount += varianceAnnualAbovegroundRemovalsCarbonCount
                summary.totalVarianceSoilCarbonEmisYear += varianceSoilCarbonEmisYearPixel
                summary.totalVarianceSoilCarbonEmisYearCount += varianceSoilCarbonEmisYearCount

                summary.totalFluxModelExtentArea += fluxModelExtentAreaPixel
              }
            updateSummary(thresholds.tail, stats.updated(pKey, summary))
          }
        }

          val updatedSummary: Map[CarbonFluxDataGroup, CarbonFluxData] =
            updateSummary(thresholds, acc.stats)

          acc = CarbonFluxSummary(updatedSummary)

        }
      }
  }
}
