package org.globalforestwatch.summarystats.annualupdate

import cats.implicits._
import geotrellis.raster.summary.GridVisitor
import geotrellis.raster._
import org.globalforestwatch.summarystats.Summary
import org.globalforestwatch.util.Geodesy
import org.globalforestwatch.util.Implicits._

import scala.annotation.tailrec

/** LossData Summary by year */
case class AnnualUpdateSummary(
  stats: Map[AnnualUpdateDataGroup, AnnualUpdateData] = Map.empty
) extends Summary[AnnualUpdateSummary] {

  /** Combine two Maps and combine their LossData when a year is present in both */
  def merge(other: AnnualUpdateSummary): AnnualUpdateSummary = {
    // the years.combine method uses LossData.lossDataSemigroup instance to perform per value combine on the map
    AnnualUpdateSummary(stats.combine(other.stats))
  }
}

object AnnualUpdateSummary {
  // TreeLossSummary form Raster[TreeLossTile] -- cell types may not be the same

  implicit val mdhCellRegisterForTreeLossRaster1
    : GridVisitor[Raster[AnnualUpdateTile], AnnualUpdateSummary]
      = new GridVisitor[Raster[AnnualUpdateTile], AnnualUpdateSummary] {
      val acc = new AnnualUpdateSummary()

      def result = acc

      def visit(raster: Raster[AnnualUpdateTile],
                   col: Int,
                   row: Int): Unit = {
        // This is a pixel by pixel operation
        val loss: Integer = raster.tile.loss.getData(col, row)
        val gain: Boolean = raster.tile.gain.getData(col, row)
        val tcd2000: Integer = raster.tile.tcd2000.getData(col, row)
        val tcd2010: Integer = raster.tile.tcd2010.getData(col, row)
        val biomass: Double = raster.tile.biomass.getData(col, row)
        val mangroveBiomass: Double =
          raster.tile.mangroveBiomass.getData(col, row)
        val drivers: String = raster.tile.drivers.getData(col, row)
        val globalLandCover: String =
          raster.tile.globalLandCover.getData(col, row)
        val primaryForest: Boolean = raster.tile.primaryForest.getData(col, row)
        val idnPrimaryForest: Boolean =
          raster.tile.idnPrimaryForest.getData(col, row)
        val erosion: String = raster.tile.erosion.getData(col, row)
        val biodiversitySignificance: Boolean =
          raster.tile.biodiversitySignificance.getData(col, row)
        val biodiversityIntactness: Boolean =
          raster.tile.biodiversityIntactness.getData(col, row)
        val wdpa: String = raster.tile.wdpa.getData(col, row)
        val aze: Boolean = raster.tile.aze.getData(col, row)
        val plantations: String = raster.tile.plantations.getData(col, row)
        val riverBasins: String = raster.tile.riverBasins.getData(col, row)
        val ecozones: String = raster.tile.ecozones.getData(col, row)
        val urbanWatersheds: Boolean =
          raster.tile.urbanWatersheds.getData(col, row)
        val mangroves1996: Boolean = raster.tile.mangroves1996.getData(col, row)
        val mangroves2016: Boolean = raster.tile.mangroves2016.getData(col, row)
        val waterStress: String = raster.tile.waterStress.getData(col, row)
        val intactForestLandscapes: String =
          raster.tile.intactForestLandscapes.getData(col, row)
        val endemicBirdAreas: Boolean =
          raster.tile.endemicBirdAreas.getData(col, row)
        val tigerLandscapes: Boolean =
          raster.tile.tigerLandscapes.getData(col, row)
        val landmark: Boolean = raster.tile.landmark.getData(col, row)
        val landRights: Boolean = raster.tile.landRights.getData(col, row)
        val keyBiodiversityAreas: Boolean =
          raster.tile.keyBiodiversityAreas.getData(col, row)
        val mining: Boolean = raster.tile.mining.getData(col, row)
        val rspo: String = raster.tile.rspo.getData(col, row)
        val peatlands: Boolean = raster.tile.peatlands.getData(col, row)
        val oilPalm: Boolean = raster.tile.oilPalm.getData(col, row)
        val idnForestMoratorium: Boolean =
          raster.tile.idnForestMoratorium.getData(col, row)
        val idnLandCover: String = raster.tile.idnLandCover.getData(col, row)
        val mexProtectedAreas: Boolean =
          raster.tile.mexProtectedAreas.getData(col, row)
        val mexPaymentForEcosystemServices: Boolean =
          raster.tile.mexPaymentForEcosystemServices.getData(col, row)
        val mexForestZoning: String =
          raster.tile.mexForestZoning.getData(col, row)
        val perProductionForest: Boolean =
          raster.tile.perProductionForest.getData(col, row)
        val perProtectedAreas: Boolean =
          raster.tile.perProtectedAreas.getData(col, row)
        val perForestConcessions: String =
          raster.tile.perForestConcessions.getData(col, row)
        val braBiomes: String = raster.tile.braBiomes.getData(col, row)
        val woodFiber: Boolean = raster.tile.woodFiber.getData(col, row)
        val resourceRights: Boolean =
          raster.tile.resourceRights.getData(col, row)
        val logging: Boolean = raster.tile.logging.getData(col, row)
        val oilGas: Boolean = raster.tile.oilGas.getData(col, row)

        //        val cols: Int = raster.rasterExtent.cols
        //        val rows: Int = raster.rasterExtent.rows
        //        val ext = raster.rasterExtent.extent
        //        val cellSize = raster.cellSize

        val lat: Double = raster.rasterExtent.gridRowToMap(row)
        val area: Double = Geodesy.pixelArea(lat, raster.cellSize) // uses Pixel's center coordiate.  +- raster.cellSize.height/2 doesn't make much of a difference

        val areaHa = area / 10000.0

        val gainArea: Double = gain * areaHa

        val co2Factor = 0.5 * 44 / 12
        val mangroveCo2Factor = 0.45 * 44 / 12

        val biomassPixel = biomass * areaHa
        val co2Pixel = biomassPixel * co2Factor
        val mangroveBiomassPixel = mangroveBiomass * areaHa
        val mangroveCo2Pixel = mangroveBiomassPixel * mangroveCo2Factor

        val thresholds = List(10, 15, 20, 25, 30, 50, 75)

        @tailrec
        def updateSummary(
          thresholds: List[Int],
          stats: Map[AnnualUpdateDataGroup, AnnualUpdateData]
        ): Map[AnnualUpdateDataGroup, AnnualUpdateData] = {
          if (thresholds == Nil) stats
          else {
            val pKey = AnnualUpdateDataGroup(
              loss,
              thresholds.head,
              drivers,
              globalLandCover,
              primaryForest,
              idnPrimaryForest,
              erosion,
              biodiversitySignificance,
              biodiversityIntactness,
              wdpa,
              aze,
              plantations,
              riverBasins,
              ecozones,
              urbanWatersheds,
              mangroves1996,
              mangroves2016,
              waterStress,
              intactForestLandscapes,
              endemicBirdAreas,
              tigerLandscapes,
              landmark,
              landRights,
              keyBiodiversityAreas,
              mining,
              rspo,
              peatlands,
              oilPalm,
              idnForestMoratorium,
              idnLandCover,
              mexProtectedAreas,
              mexPaymentForEcosystemServices,
              mexForestZoning,
              perProductionForest,
              perProtectedAreas,
              perForestConcessions,
              braBiomes,
              woodFiber,
              resourceRights,
              logging,
              oilGas
            )

            val summary: AnnualUpdateData =
              stats.getOrElse(
                key = pKey,
                default =
                  AnnualUpdateData(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
              )

            summary.totalArea += areaHa
            summary.totalGainArea += gainArea

            if (tcd2000 >= thresholds.head) {

              if (loss != null) {
                summary.treecoverLoss += areaHa
                summary.biomassLoss += biomassPixel
                summary.co2Emissions += co2Pixel
                summary.mangroveBiomassLoss += mangroveBiomassPixel
                summary.mangroveCo2Emissions += mangroveCo2Pixel
              }

              summary.treecoverExtent2000 += areaHa
              summary.totalBiomass += biomassPixel
              summary.totalCo2 += co2Pixel
              summary.totalMangroveBiomass += mangroveBiomassPixel
              summary.totalMangroveCo2 += mangroveCo2Pixel
            }

            if (tcd2010 >= thresholds.head) {
              summary.treecoverExtent2010 += areaHa
            }

            updateSummary(thresholds.tail, stats.updated(pKey, summary))
          }
        }

        val updatedSummary: Map[AnnualUpdateDataGroup, AnnualUpdateData] =
          updateSummary(thresholds, acc.stats)
      }
    }
}
