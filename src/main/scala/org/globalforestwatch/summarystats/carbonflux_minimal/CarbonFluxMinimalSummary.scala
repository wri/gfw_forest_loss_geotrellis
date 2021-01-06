package org.globalforestwatch.summarystats.carbonflux_minimal

import cats.data.NonEmptyList
import cats.implicits._
import geotrellis.raster.summary.GridVisitor
import geotrellis.raster._
import org.globalforestwatch.summarystats.Summary
import org.globalforestwatch.util.Geodesy
import org.globalforestwatch.util.Util.getAnyMapValue
import org.globalforestwatch.util.Implicits._

import scala.annotation.tailrec

/** LossData Summary by year */
case class CarbonFluxMinimalSummary(stats: Map[CarbonFluxMinimalDataGroup, CarbonFluxMinimalData] =
                           Map.empty)
  extends Summary[CarbonFluxMinimalSummary] {

  /** Combine two Maps and combine their LossData when a year is present in both */
  def merge(other: CarbonFluxMinimalSummary): CarbonFluxMinimalSummary = {
    // the years.combine method uses LossData.lossDataSemigroup instance to perform per value combine on the map
    CarbonFluxMinimalSummary(stats.combine(other.stats))
  }
}

object CarbonFluxMinimalSummary {
  // TreeLossSummary form Raster[TreeLossTile] -- cell types may not be the same

  def getGridVisitor(kwargs: Map[String, Any]): GridVisitor[Raster[CarbonFluxMinimalTile], CarbonFluxMinimalSummary] =
    new GridVisitor[Raster[CarbonFluxMinimalTile], CarbonFluxMinimalSummary] {
      private var acc: CarbonFluxMinimalSummary = new CarbonFluxMinimalSummary()

      def result: CarbonFluxMinimalSummary = acc

      def visit(raster: Raster[CarbonFluxMinimalTile],
                col: Int,
                row: Int): Unit = {

        val tcdYear: Int = getAnyMapValue[Int](kwargs, "tcdYear")

        // This is a pixel by pixel operation
        val loss: Integer = raster.tile.loss.getData(col, row)
        val gain: Boolean = raster.tile.gain.getData(col, row)
        val tcd2000: Integer = raster.tile.tcd2000.getData(col, row)
        val tcd2010: Integer = raster.tile.tcd2010.getData(col, row)
        val biomass: Double = raster.tile.biomass.getData(col, row)

        val grossCumulAbovegroundRemovalsCo2: Float = raster.tile.grossCumulAbovegroundRemovalsCo2.getData(col, row)
        val grossCumulBelowgroundRemovalsCo2: Float = raster.tile.grossCumulBelowgroundRemovalsCo2.getData(col, row)
        val grossEmissionsCo2eNonCo2: Float = raster.tile.grossEmissionsCo2eNonCo2.getData(col, row)
        val grossEmissionsCo2eCo2Only: Float =  raster.tile.grossEmissionsCo2eCo2Only.getData(col, row)
        val netFluxCo2: Float = raster.tile.netFluxCo2.getData(col, row)
        val fluxModelExtent: Boolean = raster.tile.fluxModelExtent.getData(col, row)

        val contextualLayers: List[String] =
          getAnyMapValue[NonEmptyList[String]](kwargs, "contextualLayers").toList

        val isPrimaryForest: Boolean = {
          if (contextualLayers contains "is__umd_regional_primary_forest_2001")
            raster.tile.primaryForest.getData(col, row)
          else false
        }

        val isPlantations: Boolean = {
          if (contextualLayers contains "is__gfw_plantations")
            raster.tile.plantationsBool.getData(col, row)
          else false
        }

        val lat: Double = raster.rasterExtent.gridRowToMap(row)
        val area: Double = Geodesy.pixelArea(lat, raster.cellSize) // uses Pixel's center coordiate.  +- raster.cellSize.height/2 doesn't make much of a difference

        val areaHa = area / 10000.0

        val gainArea: Double = gain * areaHa

        val biomassPixel = biomass * areaHa

        val grossCumulAbovegroundRemovalsCo2Pixel = grossCumulAbovegroundRemovalsCo2 * areaHa
        val grossCumulBelowgroundRemovalsCo2Pixel = grossCumulBelowgroundRemovalsCo2 * areaHa
        val grossCumulAboveBelowgroundRemovalsCo2Pixel = grossCumulAbovegroundRemovalsCo2Pixel + grossCumulBelowgroundRemovalsCo2Pixel

        val grossEmissionsCo2eNonCo2Pixel = grossEmissionsCo2eNonCo2 * areaHa
        val grossEmissionsCo2eCo2OnlyPixel = grossEmissionsCo2eCo2Only * areaHa
        val grossEmissionsCo2eAllGases = grossEmissionsCo2eNonCo2 + grossEmissionsCo2eCo2Only
        val grossEmissionsCo2eAllGasesPixel = grossEmissionsCo2eNonCo2Pixel + grossEmissionsCo2eCo2OnlyPixel

        val netFluxCo2Pixel = netFluxCo2 * areaHa

        val fluxModelExtentAreaInt: Integer = if (fluxModelExtent) 1 else 0
        val fluxModelExtentAreaPixel: Double = fluxModelExtentAreaInt * areaHa

        val thresholds: List[Int] =
           getAnyMapValue[NonEmptyList[Int]](kwargs, "thresholdFilter").toList

        @tailrec
        def updateSummary(
                           thresholds: List[Int],
                           stats: Map[CarbonFluxMinimalDataGroup, CarbonFluxMinimalData]
                         ): Map[CarbonFluxMinimalDataGroup, CarbonFluxMinimalData] = {
          if (thresholds == Nil) stats
          else {
            val pKey =
              CarbonFluxMinimalDataGroup(
                thresholds.head,
                tcdYear,
                isPrimaryForest,
                isPlantations
              )

            val summary: CarbonFluxMinimalData =
              stats.getOrElse(
                key = pKey,
                default =
                  CarbonFluxMinimalData(CarbonFluxMinimalYearDataMap.empty,
                    0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0,
                    0)
              )

            summary.totalArea += areaHa
            summary.totalGainArea += gainArea

            if ((tcd2000 >= thresholds.head && tcdYear == 2000) || (tcd2010 >= thresholds.head && tcdYear == 2010)) {

              if (loss != null) {
                summary.lossYear(loss).treecoverLoss += areaHa
                summary.lossYear(loss).biomassLoss += biomassPixel
                summary.lossYear(loss).grossEmissionsCo2eCo2Only += grossEmissionsCo2eCo2OnlyPixel
                summary.lossYear(loss).grossEmissionsCo2eNonCo2 += grossEmissionsCo2eNonCo2Pixel
                summary.lossYear(loss).grossEmissionsCo2eAllGases += grossEmissionsCo2eAllGasesPixel
              }

              tcdYear match {
                case 2000 => summary.treecoverExtent2000 += areaHa
                case 2010 => summary.treecoverExtent2010 += areaHa
              }
              summary.totalBiomass += biomassPixel

              summary.totalGrossCumulAbovegroundRemovalsCo2 += grossCumulAbovegroundRemovalsCo2Pixel
              summary.totalGrossCumulBelowgroundRemovalsCo2 += grossCumulBelowgroundRemovalsCo2Pixel
              summary.totalGrossCumulAboveBelowgroundRemovalsCo2 += grossCumulAboveBelowgroundRemovalsCo2Pixel

              summary.totalGrossEmissionsCo2eCo2Only += grossEmissionsCo2eCo2OnlyPixel
              summary.totalGrossEmissionsCo2eNonCo2 += grossEmissionsCo2eNonCo2Pixel
              summary.totalGrossEmissionsCo2eAllGases += grossEmissionsCo2eAllGasesPixel

              summary.totalNetFluxCo2 += netFluxCo2Pixel

              summary.totalFluxModelExtentArea += fluxModelExtentAreaPixel

            }

            tcdYear match {
              case 2000 =>
                if (tcd2010 >= thresholds.head)
                  summary.treecoverExtent2010 += areaHa
              case 2010 =>
                if (tcd2000 >= thresholds.head)
                  summary.treecoverExtent2000 += areaHa
            }

            updateSummary(thresholds.tail, stats.updated(pKey, summary))
          }
        }

        val updatedSummary: Map[CarbonFluxMinimalDataGroup, CarbonFluxMinimalData] =
          updateSummary(thresholds, acc.stats)

        acc = CarbonFluxMinimalSummary(updatedSummary)

      }
    }
}
