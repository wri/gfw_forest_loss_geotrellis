package org.globalforestwatch.carbonflux

import geotrellis.contrib.polygonal.CellVisitor
import geotrellis.raster._
import cats.implicits._
import org.globalforestwatch.util.{Geodesy, Summary}
import geotrellis.raster.histogram.StreamingHistogram

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

  implicit val mdhCellRegisterForCarbonFluxRaster1
    : CellVisitor[Raster[CarbonFluxTile], CarbonFluxSummary] =
    new CellVisitor[Raster[CarbonFluxTile], CarbonFluxSummary] {

      def register(raster: Raster[CarbonFluxTile],
                   col: Int,
                   row: Int,
                   acc: CarbonFluxSummary): CarbonFluxSummary = {
        // This is a pixel by pixel operation
        val loss: Integer = raster.tile.loss.getData(col, row)
        val tcd2000: Integer = raster.tile.tcd2000.getData(col, row)
        val biomass: Double = raster.tile.biomass.getData(col, row)

        val grossAnnualRemovalsCarbon: Double =
          raster.tile.grossAnnualRemovalsCarbon.getData(col, row)
        val grossCumulRemovalsCarbon: Double =
          raster.tile.grossCumulRemovalsCarbon.getData(col, row)
        val netFluxCo2: Double = raster.tile.netFluxCo2.getData(col, row)
        val agcEmisYear: Double = raster.tile.agcEmisYear.getData(col, row)
        val bgcEmisYear: Double = raster.tile.bgcEmisYear.getData(col, row)
        val deadwoodCarbonEmisYear: Double =
          raster.tile.deadwoodCarbonEmisYear.getData(col, row)
        val litterCarbonEmisYear: Double =
          raster.tile.litterCarbonEmisYear.getData(col, row)
        val soilCarbonEmisYear: Double =
          raster.tile.soilCarbonEmisYear.getData(col, row)
        val totalCarbonEmisYear: Double =
          raster.tile.totalCarbonEmisYear.getData(col, row)
        val agc2000: Double = raster.tile.agc2000.getData(col, row)
        val bgc2000: Double = raster.tile.bgc2000.getData(col, row)
        val deadwoodCarbon2000: Double =
          raster.tile.deadwoodCarbon2000.getData(col, row)
        val litterCarbon2000: Double =
          raster.tile.litterCarbon2000.getData(col, row)
        val soilCarbon2000: Double =
          raster.tile.soilCarbon2000.getData(col, row)
        val totalCarbon2000: Double =
          raster.tile.totalCarbon2000.getData(col, row)
        val grossEmissionsCo2: Double =
          raster.tile.grossEmissionsCo2.getData(col, row)

        val gain: Integer = raster.tile.gain.getData(col, row)
        val mangroveBiomassExtent: Boolean =
          raster.tile.mangroveBiomassExtent.getData(col, row)
        val drivers: String = raster.tile.drivers.getData(col, row)
        val wdpa: String = raster.tile.wdpa.getData(col, row)
        val plantations: String = raster.tile.plantations.getData(col, row)
        val ecozones: String = raster.tile.ecozones.getData(col, row)
        val intactForestLandscapes: String =
          raster.tile.intactForestLandscapes.getData(col, row)
        val landRights: Boolean = raster.tile.landRights.getData(col, row)
        val primaryForest: Boolean = raster.tile.primaryForest.getData(col, row)

        val cols: Int = raster.rasterExtent.cols
        val rows: Int = raster.rasterExtent.rows
        val ext = raster.rasterExtent.extent
        val cellSize = raster.cellSize

        val lat: Double = raster.rasterExtent.gridRowToMap(row)
        val area: Double = Geodesy.pixelArea(lat, raster.cellSize) // uses Pixel's center coordiate.  +- raster.cellSize.height/2 doesn't make much of a difference

        val areaHa = area / 10000.0

        val biomassPixel = biomass * areaHa
        val grossAnnualRemovalsCarbonPixel = grossAnnualRemovalsCarbon * areaHa
        val grossCumulRemovalsCarbonPixel = grossCumulRemovalsCarbon * areaHa
        val netFluxCo2Pixel = netFluxCo2 * areaHa
        val agcEmisYearPixel = agcEmisYear * areaHa
        val bgcEmisYearPixel = bgcEmisYear * areaHa
        val deadwoodCarbonEmisYearPixel = deadwoodCarbonEmisYear * areaHa
        val litterCarbonEmisYearPixel = litterCarbonEmisYear * areaHa
        val soilCarbonEmisYearPixel = soilCarbonEmisYear * areaHa
        val totalCarbonEmisYearPixel = totalCarbonEmisYear * areaHa
        val agc2000Pixel = agc2000 * areaHa
        val bgc2000Pixel = bgc2000 * areaHa
        val deadwoodCarbon2000Pixel = deadwoodCarbon2000 * areaHa
        val litterCarbon2000Pixel = litterCarbon2000 * areaHa
        val soilCarbon2000Pixel = soilCarbon2000 * areaHa
        val totalCarbon2000Pixel = totalCarbon2000 * areaHa
        val grossEmissionsCo2Pixel = grossEmissionsCo2 * areaHa

        val thresholds = List(0, 10, 15, 20, 25, 30, 50, 75)

        def updateSummary(
          thresholds: List[Int],
          stats: Map[CarbonFluxDataGroup, CarbonFluxData]
        ): Map[CarbonFluxDataGroup, CarbonFluxData] = {
          if (thresholds == Nil) stats
          else {
            val pKey = CarbonFluxDataGroup(
              thresholds.head,
              gain,
              mangroveBiomassExtent,
              drivers,
              ecozones,
              landRights,
              wdpa,
              intactForestLandscapes,
              plantations,
              primaryForest
            )

            val summary: CarbonFluxData =
              stats.getOrElse(
                key = pKey,
                default = CarbonFluxData(
                  CarbonFluxYearDataMap.empty,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0
                )
              )

            summary.totalArea += areaHa

            if (tcd2000 >= thresholds.head) {

              if (loss != null) {
                summary.lossYear(loss).area_loss += areaHa
                summary.lossYear(loss).biomass_loss += biomassPixel
                summary
                  .lossYear(loss)
                  .gross_emissions_co2 += grossEmissionsCo2Pixel
              }

              summary.extent2000 += areaHa
              summary.totalBiomass += biomassPixel
              summary.avgBiomass = ((summary.avgBiomass * summary.count) + biomass) / (summary.count + 1)
              summary.totalGrossAnnualRemovalsCarbon += grossAnnualRemovalsCarbonPixel
              summary.avgGrossAnnualRemovalsCarbon = ((summary.avgGrossAnnualRemovalsCarbon * summary.count) + grossAnnualRemovalsCarbon) / (summary.count + 1)
              summary.totalGrossCumulRemovalsCarbon += grossCumulRemovalsCarbonPixel
              summary.avgGrossCumulRemovalsCarbon = ((summary.avgGrossCumulRemovalsCarbon * summary.count) + grossCumulRemovalsCarbon) / (summary.count + 1)
              summary.totalNetFluxCo2 += netFluxCo2Pixel
              summary.avgNetFluxCo2 = ((summary.avgNetFluxCo2 * summary.count) + netFluxCo2) / (summary.count + 1)

              summary.totalAgcEmisYear += agcEmisYearPixel
              summary.avgAgcEmisYear = ((summary.avgAgcEmisYear * summary.count) + agcEmisYear) / (summary.count + 1)

              summary.totalBgcEmisYear += bgcEmisYearPixel
              summary.avgBgcEmisYear = ((summary.avgBgcEmisYear * summary.count) + bgcEmisYear) / (summary.count + 1)

              summary.totalDeadwoodCarbonEmisYear += deadwoodCarbonEmisYearPixel
              summary.avgDeadwoodCarbonEmisYear = ((summary.avgDeadwoodCarbonEmisYear * summary.count) + deadwoodCarbonEmisYear) / (summary.count + 1)

              summary.totalLitterCarbonEmisYear += litterCarbonEmisYearPixel
              summary.avgLitterCarbonEmisYear = ((summary.avgLitterCarbonEmisYear * summary.count) + litterCarbonEmisYear) / (summary.count + 1)

              summary.totalSoilCarbonEmisYear += soilCarbonEmisYearPixel
              summary.avgSoilCarbonEmisYear = ((summary.avgSoilCarbonEmisYear * summary.count) + soilCarbonEmisYear) / (summary.count + 1)

              summary.totalCarbonEmisYear += totalCarbonEmisYearPixel
              summary.avgTotalCarbonEmisYear = ((summary.avgTotalCarbonEmisYear * summary.count) + totalCarbonEmisYear) / (summary.count + 1)

              summary.totalAgc2000 += agc2000Pixel
              summary.avgAgc2000 = ((summary.avgAgc2000 * summary.count) + agc2000) / (summary.count + 1)
              summary.totalBgc2000 += bgc2000Pixel
              summary.avgBgc2000 = ((summary.avgBgc2000 * summary.count) + bgc2000) / (summary.count + 1)
              summary.totalDeadwoodCarbon2000 += deadwoodCarbon2000Pixel
              summary.avgDeadwoodCarbon2000 = ((summary.avgDeadwoodCarbon2000 * summary.count) + deadwoodCarbon2000) / (summary.count + 1)
              summary.totalLitterCarbon2000 += litterCarbon2000Pixel
              summary.avgLitterCarbon2000 = ((summary.avgLitterCarbon2000 * summary.count) + litterCarbon2000) / (summary.count + 1)
              summary.totalSoil2000Year += soilCarbon2000Pixel
              summary.avgSoilCarbon2000 = ((summary.avgSoilCarbon2000 * summary.count) + soilCarbon2000) / (summary.count + 1)
              summary.totalCarbon2000 += totalCarbon2000Pixel
              summary.avgTotalCarbon2000 = ((summary.avgTotalCarbon2000 * summary.count) + totalCarbon2000) / (summary.count + 1)
              summary.totalGrossEmissionsCo2 += grossEmissionsCo2Pixel
              summary.avgGrossEmissionsCo2 = ((summary.avgGrossEmissionsCo2 * summary.count) + grossEmissionsCo2) / (summary.count + 1)
              summary.count += 1

            }

            updateSummary(thresholds.tail, stats.updated(pKey, summary))
          }
        }

        val updatedSummary: Map[CarbonFluxDataGroup, CarbonFluxData] =
          updateSummary(thresholds, acc.stats)

        CarbonFluxSummary(updatedSummary)

      }
    }
}
