package org.globalforestwatch.summarystats.treecoverloss

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
case class TreeLossSummary(stats: Map[TreeLossDataGroup, TreeLossData] =
                           Map.empty)
  extends Summary[TreeLossSummary] {

  /** Combine two Maps and combine their LossData when a year is present in both */
  def merge(other: TreeLossSummary): TreeLossSummary = {
    // the years.combine method uses LossData.lossDataSemigroup instance to perform per value combine on the map
    TreeLossSummary(stats.combine(other.stats))
  }
  def isEmpty = stats.isEmpty
}

object TreeLossSummary {
  // TreeLossSummary form Raster[TreeLossTile] -- cell types may not be the same

  def getGridVisitor(kwargs: Map[String, Any]): GridVisitor[Raster[TreeLossTile], TreeLossSummary] =
    new GridVisitor[Raster[TreeLossTile], TreeLossSummary] {
      private var acc: TreeLossSummary = new TreeLossSummary()

      def result: TreeLossSummary = acc

      def visit(raster: Raster[TreeLossTile],
                   col: Int,
                   row: Int): Unit = {

        val tcdYear: Int = getAnyMapValue[Int](kwargs, "tcdYear")

        // This is a pixel by pixel operation
        val loss: Integer = raster.tile.loss.getData(col, row)
        val gain: Boolean = raster.tile.gain.getData(col, row)
        val tcd2000: Integer = raster.tile.tcd2000.getData(col, row)
        val tcd2010: Integer = raster.tile.tcd2010.getData(col, row)
        val biomass: Double = raster.tile.biomass.getData(col, row)


        // Optionally calculate stocks in carbon pools in 2000
        val carbonPools: Boolean =
          getAnyMapValue[Boolean](kwargs, "carbonPools")

        val agc2000: Float = raster.tile.agc2000.getData(col, row)
        val bgc2000: Float = raster.tile.bgc2000.getData(col, row)
        val soilCarbon2000: Float = raster.tile.soilCarbon2000.getData(col, row)

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
            raster.tile.plantedForestsBool.getData(col, row)
          else false
        }

        val plantationsPre2000: Boolean =
          raster.tile.plantationsPre2000.getData(col, row)

        val lat: Double = raster.rasterExtent.gridRowToMap(row)
        val area: Double = Geodesy.pixelArea(lat, raster.cellSize) // uses Pixel's center coordinate.  +- raster.cellSize.height/2 doesn't make much of a difference

        val areaHa = area / 10000.0

        val gainArea: Double = gain * areaHa
        val biomassPixel = biomass * areaHa
        val agc2000Pixel = agc2000 * areaHa
        val bgc2000Pixel = bgc2000 * areaHa
        val soilCarbon2000Pixel = soilCarbon2000 * areaHa

        val grossCumulAbovegroundRemovalsCo2Pixel = grossCumulAbovegroundRemovalsCo2 * areaHa
        val grossCumulBelowgroundRemovalsCo2Pixel = grossCumulBelowgroundRemovalsCo2 * areaHa
        val grossCumulAboveBelowgroundRemovalsCo2Pixel = grossCumulAbovegroundRemovalsCo2Pixel + grossCumulBelowgroundRemovalsCo2Pixel

        val grossEmissionsCo2eNonCo2Pixel = grossEmissionsCo2eNonCo2 * areaHa
        val grossEmissionsCo2eCo2OnlyPixel = grossEmissionsCo2eCo2Only * areaHa
        val grossEmissionsCo2eAllGasesPixel = grossEmissionsCo2eNonCo2Pixel + grossEmissionsCo2eCo2OnlyPixel

        val netFluxCo2Pixel = netFluxCo2 * areaHa

        val fluxModelExtentAreaInt: Integer = if (fluxModelExtent) 1 else 0
        val fluxModelExtentAreaPixel: Double = fluxModelExtentAreaInt * areaHa

        val thresholds: List[Int] =
           getAnyMapValue[NonEmptyList[Int]](kwargs, "thresholdFilter").toList

        @tailrec
        def updateSummary(
                           thresholds: List[Int],
                           stats: Map[TreeLossDataGroup, TreeLossData]
                         ): Map[TreeLossDataGroup, TreeLossData] = {
          if (thresholds == Nil) stats
          else {
            val pKey =
              TreeLossDataGroup(
                thresholds.head,
                tcdYear,
                isPrimaryForest,
                isPlantations,
                gain
              )

            val summary: TreeLossData =
              stats.getOrElse(
                key = pKey,
                default =
                  TreeLossData(TreeLossYearDataMap.empty, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
              )

            summary.totalArea += areaHa
            summary.totalGainArea += gainArea

            if (((thresholds.head == 0 || tcd2000 > thresholds.head) && tcdYear == 2000) || ((thresholds.head == 0 || tcd2010 > thresholds.head) && tcdYear == 2010))
            {

              if (loss != null) {
                summary.lossYear(loss).treecoverLoss += areaHa
                summary.lossYear(loss).biomassLoss += biomassPixel

                if (!plantationsPre2000) {
                  summary.lossYear(loss).grossEmissionsCo2eCo2Only += grossEmissionsCo2eCo2OnlyPixel
                  summary.lossYear(loss).grossEmissionsCo2eNonCo2 += grossEmissionsCo2eNonCo2Pixel
                  summary.lossYear(loss).grossEmissionsCo2eAllGases += grossEmissionsCo2eAllGasesPixel
                }
              }

              // TODO: use extent2010 to calculate avg biomass incase year is selected
              summary.avgBiomass = ((summary.avgBiomass * summary.treecoverExtent2000) + (biomass * areaHa)) / (summary.treecoverExtent2000 + areaHa)
              tcdYear match {
                case 2000 => summary.treecoverExtent2000 += areaHa
                case 2010 => summary.treecoverExtent2010 += areaHa
              }
              summary.totalBiomass += biomassPixel

              if (carbonPools) {
                summary.totalAgc2000 += agc2000Pixel
                summary.totalBgc2000 += bgc2000Pixel
                summary.totalSoilCarbon2000 += soilCarbon2000Pixel
              }

              if (!plantationsPre2000) {
                summary.totalGrossCumulAbovegroundRemovalsCo2 += grossCumulAbovegroundRemovalsCo2Pixel
                summary.totalGrossCumulBelowgroundRemovalsCo2 += grossCumulBelowgroundRemovalsCo2Pixel
                summary.totalGrossCumulAboveBelowgroundRemovalsCo2 += grossCumulAboveBelowgroundRemovalsCo2Pixel

                summary.totalGrossEmissionsCo2eCo2Only += grossEmissionsCo2eCo2OnlyPixel
                summary.totalGrossEmissionsCo2eNonCo2 += grossEmissionsCo2eNonCo2Pixel
                summary.totalGrossEmissionsCo2eAllGases += grossEmissionsCo2eAllGasesPixel

                summary.totalNetFluxCo2 += netFluxCo2Pixel

                summary.totalFluxModelExtentArea += fluxModelExtentAreaPixel
              }
            } else if (gain && !plantationsPre2000) {
            // Adds the gain pixels that don't have any tree cover density to the flux model outputs to get
            // the correct flux model outputs (TCD>=threshold OR Hansen gain)
              summary.totalGrossCumulAbovegroundRemovalsCo2 += grossCumulAbovegroundRemovalsCo2Pixel
              summary.totalGrossCumulBelowgroundRemovalsCo2 += grossCumulBelowgroundRemovalsCo2Pixel
              summary.totalGrossCumulAboveBelowgroundRemovalsCo2 += grossCumulAboveBelowgroundRemovalsCo2Pixel

              summary.totalGrossEmissionsCo2eCo2Only += grossEmissionsCo2eCo2OnlyPixel
              summary.totalGrossEmissionsCo2eNonCo2 += grossEmissionsCo2eNonCo2Pixel
              summary.totalGrossEmissionsCo2eAllGases += grossEmissionsCo2eAllGasesPixel

              summary.totalNetFluxCo2 += netFluxCo2Pixel

              summary.totalFluxModelExtentArea += fluxModelExtentAreaPixel

              if (loss != null) {
                summary.lossYear(loss).grossEmissionsCo2eCo2Only += grossEmissionsCo2eCo2OnlyPixel
                summary.lossYear(loss).grossEmissionsCo2eNonCo2 += grossEmissionsCo2eNonCo2Pixel
                summary.lossYear(loss).grossEmissionsCo2eAllGases += grossEmissionsCo2eAllGasesPixel
              }
            }

            updateSummary(thresholds.tail, stats.updated(pKey, summary))
          }
        }

        val updatedSummary: Map[TreeLossDataGroup, TreeLossData] =
          updateSummary(thresholds, acc.stats)

        acc = TreeLossSummary(updatedSummary)
      }
    }
}
