package org.globalforestwatch.summarystats.carbonflux_custom_area

import cats.implicits._
import geotrellis.contrib.polygonal.CellVisitor
import geotrellis.raster._
import org.globalforestwatch.summarystats.Summary
import org.globalforestwatch.util.Geodesy

import scala.annotation.tailrec

/** LossData Summary by year */
case class CarbonCustomSummary(
                              stats: Map[CarbonCustomDataGroup, CarbonCustomData] = Map.empty
                            ) extends Summary[CarbonCustomSummary] {

  /** Combine two Maps and combine their LossData when a year is present in both */
  def merge(other: CarbonCustomSummary): CarbonCustomSummary = {
    // the years.combine method uses LossData.lossDataSemigroup instance to perform per value combine on the map
    CarbonCustomSummary(stats.combine(other.stats))
  }
}

object CarbonCustomSummary {
  // CarbonFluxSummary form Raster[CarbonFluxTile] -- cell types may not be the same

  implicit val mdhCellRegisterForCarbonFluxRaster1: CellVisitor[Raster[CarbonCustomTile], CarbonCustomSummary] =
    new CellVisitor[Raster[CarbonCustomTile], CarbonCustomSummary] {

      def register(
                    raster: Raster[CarbonCustomTile],
                    col: Int,
                    row: Int,
                    acc: CarbonCustomSummary
                  ): CarbonCustomSummary = {
        // This is a pixel by pixel operation
        val lossYear: Integer = raster.tile.loss.getData(col, row)
        val tcd2000: Integer = raster.tile.tcd2000.getData(col, row)
        val biomass: Double = raster.tile.biomass.getData(col, row)

        val grossAnnualRemovalsCarbon: Float =
          raster.tile.grossAnnualRemovalsCarbon.getData(col, row)
        val grossCumulRemovalsCarbon: Float =
          raster.tile.grossCumulRemovalsCarbon.getData(col, row)
        val netFluxCo2: Float = raster.tile.netFluxCo2.getData(col, row)
        val grossEmissionsCo2eNoneCo2: Float =
        raster.tile.grossEmissionsCo2eNoneCo2.getData(col, row)
        val grossEmissionsCo2eCo2Only: Float =
          raster.tile.grossEmissionsCo2eCo2Only.getData(col, row)

        val carbonFluxCustomArea1: Integer = raster.tile.carbonFluxCustomArea1.getData(col, row)

        //        val cols: Int = raster.rasterExtent.cols
        //        val rows: Int = raster.rasterExtent.rows
        //        val ext = raster.rasterExtent.extent
        //        val cellSize = raster.cellSize

        val lat: Double = raster.rasterExtent.gridRowToMap(row)
        val area: Double = Geodesy.pixelArea(lat, raster.cellSize) // uses Pixel's center coordiate.  +- raster.cellSize.height/2 doesn't make much of a difference

        val areaHa = area / 10000.0

        val carbonfluxLossYear: Integer = if (lossYear != null && lossYear >= 2001 && lossYear <= 2015) lossYear else null

        val biomassPixel = biomass * areaHa
        val grossAnnualRemovalsCarbonPixel = grossAnnualRemovalsCarbon * areaHa
        val grossCumulRemovalsCarbonPixel = grossCumulRemovalsCarbon * areaHa
        val netFluxCo2Pixel = netFluxCo2 * areaHa
        val grossEmissionsCo2eNoneCo2Pixel = grossEmissionsCo2eNoneCo2 * areaHa
        val grossEmissionsCo2eCo2OnlyPixel = grossEmissionsCo2eCo2Only * areaHa
        val grossEmissionsCo2e = grossEmissionsCo2eNoneCo2 + grossEmissionsCo2eCo2Only
        val grossEmissionsCo2ePixel = grossEmissionsCo2e * areaHa

        val thresholds = List(0, 10, 15, 20, 25, 30, 50, 75)


        @tailrec
        def updateSummary(
                           thresholds: List[Int],
                           stats: Map[CarbonCustomDataGroup, CarbonCustomData]
                         ): Map[CarbonCustomDataGroup, CarbonCustomData] = {
          if (thresholds == Nil) stats
          else {
            val pKey = CarbonCustomDataGroup(
              carbonfluxLossYear,
              thresholds.head,
              carbonFluxCustomArea1
            )

            val summary: CarbonCustomData =
              stats.getOrElse(
                key = pKey,
                default = CarbonCustomData(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
              )

            summary.totalArea += areaHa

            if (tcd2000 >= thresholds.head) {

              if (carbonfluxLossYear != null) {
                summary.totalTreecoverLoss += areaHa
                summary.totalBiomassLoss += biomassPixel
                summary.totalGrossEmissionsCo2eCo2Only += grossEmissionsCo2eCo2OnlyPixel
                summary.totalGrossEmissionsCo2eNoneCo2 += grossEmissionsCo2eNoneCo2Pixel
                summary.totalGrossEmissionsCo2e += grossEmissionsCo2ePixel
              }

              summary.totalTreecoverExtent2000 += areaHa
              summary.totalBiomass += biomassPixel
              summary.totalGrossAnnualRemovalsCarbon += grossAnnualRemovalsCarbonPixel
              summary.totalGrossCumulRemovalsCarbon += grossCumulRemovalsCarbonPixel
              summary.totalNetFluxCo2 += netFluxCo2Pixel
            }
            updateSummary(thresholds.tail, stats.updated(pKey, summary))
          }
        }

        val updatedSummary: Map[CarbonCustomDataGroup, CarbonCustomData] =
          updateSummary(thresholds, acc.stats)

        CarbonCustomSummary(updatedSummary)

      }
    }
}
