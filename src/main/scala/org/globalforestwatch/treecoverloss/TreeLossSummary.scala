package org.globalforestwatch.treecoverloss

import geotrellis.contrib.polygonal.CellVisitor
import geotrellis.raster._
import cats.implicits._
import org.globalforestwatch.util.Geodesy
import geotrellis.raster.histogram.StreamingHistogram


/** LossData Summary by year */
case class TreeLossSummary(stats: Map[LossDataGroup, LossData] = Map.empty) {
  /** Combine two Maps and combine their LossData when a year is present in both */
  def merge(other: TreeLossSummary): TreeLossSummary = {
    // the years.combine method uses LossData.lossDataSemigroup instance to perform per value combine on the map
    TreeLossSummary(stats.combine(other.stats))
  }
}

object TreeLossSummary {
  // TreeLossSummary form Raster[TreeLossTile] -- cell types may not be the same


  implicit val mdhCellRegisterForTreeLossRaster1: CellVisitor[Raster[TreeLossTile], TreeLossSummary] =
    new CellVisitor[Raster[TreeLossTile], TreeLossSummary] {

      def register(raster: Raster[TreeLossTile], col: Int, row: Int, acc: TreeLossSummary): TreeLossSummary = {
        // This is a pixel by pixel operation
        val loss: Integer = raster.tile.loss.getData(col, row)
        val gain: Integer = raster.tile.gain.getData(col, row)
        val tcd2000: Integer = raster.tile.tcd2000.getData(col, row)
        val tcd2010: Integer = raster.tile.tcd2010.getData(col, row)
        //  val co2Pixel: Double = raster.tile.co2Pixel.getData(col, row)
        val biomass: Double = raster.tile.biomass.getData(col, row)
        val mangroveBiomass: Double = raster.tile.mangroveBiomass.getData(col, row)
        val drivers: String = raster.tile.drivers.getData(col, row)
        val globalLandCover: String = raster.tile.globalLandCover.getData(col, row)
        val primaryForest: Boolean = raster.tile.primaryForest.getData(col, row)
        val idnPrimaryForest: Boolean = raster.tile.idnPrimaryForest.getData(col, row)
        val erosion: String = raster.tile.erosion.getData(col, row)
        val biodiversitySignificance: Boolean = raster.tile.biodiversitySignificance.getData(col, row)
        val biodiversityIntactness: Boolean = raster.tile.biodiversityIntactness.getData(col, row)
        val wdpa: String = raster.tile.wdpa.getData(col, row)
        val aze: Boolean = raster.tile.aze.getData(col, row)
        val plantations: String = raster.tile.plantations.getData(col, row)
        val riverBasins: String = raster.tile.riverBasins.getData(col, row)
        val ecozones: String = raster.tile.ecozones.getData(col, row)
        val urbanWatersheds: Boolean = raster.tile.urbanWatersheds.getData(col, row)
        val mangroves1996: Boolean = raster.tile.mangroves1996.getData(col, row)
        val mangroves2016: Boolean = raster.tile.mangroves2016.getData(col, row)
        val waterStress: String = raster.tile.waterStress.getData(col, row)
        val intactForestLandscapes: Integer = raster.tile.intactForestLandscapes.getData(col, row)
        val endemicBirdAreas: Boolean = raster.tile.endemicBirdAreas.getData(col, row)
        val tigerLandscapes: Boolean = raster.tile.tigerLandscapes.getData(col, row)
        val landmark: Boolean = raster.tile.landmark.getData(col, row)
        val landRights: Boolean = raster.tile.landRights.getData(col, row)
        val keyBiodiversityAreas: Boolean = raster.tile.keyBiodiversityAreas.getData(col, row)
        val mining: Boolean = raster.tile.mining.getData(col, row)
        val rspo: String = raster.tile.rspo.getData(col, row)
        val peatlands: Boolean = raster.tile.peatlands.getData(col, row)
        val oilPalm: Boolean = raster.tile.oilPalm.getData(col, row)
        val idnForestMoratorium: Boolean = raster.tile.idnForestMoratorium.getData(col, row)
        val idnLandCover: String = raster.tile.idnLandCover.getData(col, row)
        val mexProtectedAreas: Boolean = raster.tile.mexProtectedAreas.getData(col, row)
        val mexPaymentForEcosystemServices: Boolean = raster.tile.mexPaymentForEcosystemServices.getData(col, row)
        val mexForestZoning: String = raster.tile.mexForestZoning.getData(col, row)
        val perProductionForest: Boolean = raster.tile.perProductionForest.getData(col, row)
        val perProtectedAreas: Boolean = raster.tile.perProtectedAreas.getData(col, row)
        val perForestConcessions: String = raster.tile.perForestConcessions.getData(col, row)
        val braBiomes: String = raster.tile.braBiomes.getData(col, row)
        val woodFiber: Boolean = raster.tile.woodFiber.getData(col, row)
        val resourceRights: Boolean = raster.tile.resourceRights.getData(col, row)
        val logging: Boolean = raster.tile.logging.getData(col, row)
        val oilGas: Boolean = raster.tile.oilGas.getData(col, row)

        val cols: Int = raster.rasterExtent.cols
        val rows: Int = raster.rasterExtent.rows
        val ext = raster.rasterExtent.extent
        val cellSize = raster.cellSize

        val lat: Double = raster.rasterExtent.gridRowToMap(row)
        val area: Double = Geodesy.pixelArea(lat, raster.cellSize) // uses Pixel's center coordiate.  +- raster.cellSize.height/2 doesn't make much of a difference
        //val area: Double = raster.tile.preArea.getData(col, row)
        val areaHa = area / 10000.0

        val gainArea: Double = gain * areaHa

        val pKey = LossDataGroup(tcd2000, tcd2010,
          drivers, globalLandCover, primaryForest, idnPrimaryForest, erosion,
          biodiversitySignificance, biodiversityIntactness,
          wdpa, aze, plantations, riverBasins, ecozones, urbanWatersheds,
          mangroves1996, mangroves2016, waterStress, intactForestLandscapes, endemicBirdAreas, tigerLandscapes,
          landmark, landRights, keyBiodiversityAreas, mining, rspo, peatlands, oilPalm, idnForestMoratorium,
          idnLandCover, mexProtectedAreas, mexPaymentForEcosystemServices, mexForestZoning, perProductionForest,
          perProtectedAreas, perForestConcessions, braBiomes, woodFiber, resourceRights, logging, oilGas)

        val summary: LossData = acc.stats.getOrElse(
          key = pKey,
          default = LossData(LossYearDataMap.empty, 0, 0, 0, 0, StreamingHistogram(size = 1750), 0, 0, StreamingHistogram(size = 1000)))



        val biomassPixel = biomass * areaHa
        val co2Pixel = ((biomass * areaHa) * 0.5) * 44 / 12
        val mangroveBiomassPixel = mangroveBiomass * areaHa
        val mangroveCo2Pixel = ((mangroveBiomass * areaHa) * 0.5) * 44 / 12

        if (loss != null) {
          summary.lossYear(loss).area_loss += areaHa
          summary.lossYear(loss).biomass_loss += biomassPixel
          summary.lossYear(loss).carbon_emissions += co2Pixel
          summary.lossYear(loss).mangrove_biomass_loss += mangroveBiomassPixel
          summary.lossYear(loss).mangrove_carbon_emissions += mangroveCo2Pixel
        }


        summary.totalArea += areaHa

        summary.totalGainArea += gainArea

        summary.totalBiomass += biomassPixel
        summary.totalCo2 += co2Pixel
        summary.biomassHistogram.countItem(biomass)

        summary.totalMangroveBiomass += mangroveBiomassPixel
        summary.totalMangroveCo2 += mangroveCo2Pixel
        summary.mangroveBiomassHistogram.countItem(mangroveBiomass)

        val updated_summary: Map[LossDataGroup, LossData] = acc.stats.updated(pKey, summary)

        TreeLossSummary(updated_summary)
      }
    }
}
