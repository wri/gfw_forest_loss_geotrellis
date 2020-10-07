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
}

object CarbonFluxSummary {
  // CarbonFluxSummary form Raster[CarbonFluxTile] -- cell types may not be the same

  implicit val mdhCellRegisterForTreeLossRaster1
    : GridVisitor[Raster[CarbonFluxTile], CarbonFluxSummary] =
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

        val grossAnnualRemovalsCarbon: Float =
          raster.tile.grossAnnualRemovalsCarbon.getData(col, row)
        val grossCumulRemovalsCarbon: Float =
          raster.tile.grossCumulRemovalsCarbon.getData(col, row)
        val netFluxCo2: Float = raster.tile.netFluxCo2.getData(col, row)
        val agcEmisYear: Float = raster.tile.agcEmisYear.getData(col, row)
        val bgcEmisYear: Float = raster.tile.bgcEmisYear.getData(col, row)
        val deadwoodCarbonEmisYear: Float =
          raster.tile.deadwoodCarbonEmisYear.getData(col, row)
        val litterCarbonEmisYear: Float =
          raster.tile.litterCarbonEmisYear.getData(col, row)
        val soilCarbonEmisYear: Float =
          raster.tile.soilCarbonEmisYear.getData(col, row)
        //        val totalCarbonEmisYear: Double =
        //          raster.tile.totalCarbonEmisYear.getData(col, row)
        val agc2000: Float = raster.tile.agc2000.getData(col, row)
        val bgc2000: Float = raster.tile.bgc2000.getData(col, row)
        val deadwoodCarbon2000: Float =
          raster.tile.deadwoodCarbon2000.getData(col, row)
        val litterCarbon2000: Float =
          raster.tile.litterCarbon2000.getData(col, row)
        val soilCarbon2000: Float =
          raster.tile.soilCarbon2000.getData(col, row)
        //        val totalCarbon2000: Double =
        //          raster.tile.totalCarbon2000.getData(col, row)
        val grossEmissionsCo2eNoneCo2: Float =
        raster.tile.grossEmissionsCo2eNoneCo2.getData(col, row)
        val grossEmissionsCo2eCo2Only: Float =
          raster.tile.grossEmissionsCo2eCo2Only.getData(col, row)

        val isGain: Boolean = raster.tile.gain.getData(col, row)
        val mangroveBiomassExtent: Boolean =
          raster.tile.mangroveBiomassExtent.getData(col, row)
        val drivers: String = raster.tile.drivers.getData(col, row)
        val wdpa: String = raster.tile.wdpa.getData(col, row)
        val plantations: String = raster.tile.plantations.getData(col, row)
        val ecozones: String = raster.tile.ecozones.getData(col, row)
        val intactForestLandscapes: String =
          raster.tile.intactForestLandscapes.getData(col, row)
        val landRights: Boolean = raster.tile.landRights.getData(col, row)
        val intactPrimaryForest: Boolean =
          raster.tile.intactPrimaryForest.getData(col, row)
        val peatlandsFlux: Boolean = raster.tile.peatlandsFlux.getData(col, row)
        val forestAgeCategory: String = raster.tile.forestAgeCategory.getData(col, row)
        val jplAGBextent: Boolean = raster.tile.jplAGBextent.getData(col, row)
        val fiaRegionsUsExtent: String = raster.tile.fiaRegionsUsExtent.getData(col, row)

        //        val cols: Int = raster.rasterExtent.cols
        //        val rows: Int = raster.rasterExtent.rows
        //        val ext = raster.rasterExtent.extent
        //        val cellSize = raster.cellSize

        val lat: Double = raster.rasterExtent.gridRowToMap(row)
        val area: Double = Geodesy.pixelArea(lat, raster.cellSize) // uses Pixel's center coordiate.  +- raster.cellSize.height/2 doesn't make much of a difference

        val areaHa = area / 10000.0

        val carbonfluxLossYear: Integer = if (lossYear != null && lossYear >= 2001 && lossYear <= 2015) lossYear else null
        val isLoss: Boolean = carbonfluxLossYear != null

        val biomassPixel = biomass * areaHa
        val grossAnnualRemovalsCarbonPixel = grossAnnualRemovalsCarbon * areaHa
        val grossCumulRemovalsCarbonPixel = grossCumulRemovalsCarbon * areaHa
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
        val grossEmissionsCo2eNoneCo2Pixel = grossEmissionsCo2eNoneCo2 * areaHa
        val grossEmissionsCo2eCo2OnlyPixel = grossEmissionsCo2eCo2Only * areaHa

        val grossEmissionsCo2e = grossEmissionsCo2eNoneCo2 + grossEmissionsCo2eCo2Only
        val grossEmissionsCo2ePixel = grossEmissionsCo2e * areaHa

        val thresholds = List(0, 10, 15, 20, 25, 30, 50, 75)


        @tailrec
        def updateSummary(
                           thresholds: List[Int],
                           stats: Map[CarbonFluxDataGroup, CarbonFluxData]
                         ): Map[CarbonFluxDataGroup, CarbonFluxData] = {
          if (thresholds == Nil) stats
          else {
            val pKey = CarbonFluxDataGroup(
              carbonfluxLossYear,
              thresholds.head,
              isGain,
              isLoss,
              mangroveBiomassExtent,
              drivers,
              ecozones,
              landRights,
              wdpa,
              intactForestLandscapes,
              plantations,
              intactPrimaryForest,
              peatlandsFlux,
              forestAgeCategory,
              jplAGBextent,
              fiaRegionsUsExtent
            )

            val summary: CarbonFluxData =
              stats.getOrElse(
                key = pKey,
                default = CarbonFluxData(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                  0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
              )

            summary.totalArea += areaHa

            if (tcd2000 >= thresholds.head) {

              if (carbonfluxLossYear != null) {
                summary.treecoverLoss += areaHa
                summary.biomassLoss += biomassPixel
                summary.grossEmissionsCo2eCo2Only += grossEmissionsCo2eCo2OnlyPixel
                summary.grossEmissionsCo2eNoneCo2 += grossEmissionsCo2eNoneCo2Pixel
                summary.grossEmissionsCo2e += grossEmissionsCo2ePixel
                summary.agcEmisYear += agcEmisYearPixel
                summary.bgcEmisYear += bgcEmisYearPixel
                summary.deadwoodCarbonEmisYear += deadwoodCarbonEmisYearPixel
                summary.litterCarbonEmisYear += litterCarbonEmisYearPixel
                summary.soilCarbonEmisYear += soilCarbonEmisYearPixel
                summary.carbonEmisYear += totalCarbonEmisYearPixel
              }
              summary.treecoverExtent2000 += areaHa
              summary.totalBiomass += biomassPixel
              summary.totalGrossAnnualRemovalsCarbon += grossAnnualRemovalsCarbonPixel
              summary.totalGrossCumulRemovalsCarbon += grossCumulRemovalsCarbonPixel
              summary.totalNetFluxCo2 += netFluxCo2Pixel
              summary.totalAgc2000 += agc2000Pixel
              summary.totalBgc2000 += bgc2000Pixel
              summary.totalDeadwoodCarbon2000 += deadwoodCarbon2000Pixel
              summary.totalLitterCarbon2000 += litterCarbon2000Pixel
              summary.totalSoil2000 += soilCarbon2000Pixel
              summary.totalCarbon2000 += totalCarbon2000Pixel
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
