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
        //val biodiversitySignificance: Boolean = raster.tile.biodiversitySignificance.getData(col, row)
        //val biodiversityIntactness: Boolean = raster.tile.biodiversityIntactness.getData(col, row)
        val wdpa: String = raster.tile.wdpa.getData(col, row)
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

        val lat:Double = raster.rasterExtent.gridRowToMap(row)
        val area: Double = Geodesy.pixelArea(lat, raster.cellSize)
        val areaPlus: Double = Geodesy.pixelArea(lat + 0.00025 / 2, raster.cellSize)
        val areaMinus: Double = Geodesy.pixelArea(lat - 0.00025 / 2, raster.cellSize)

        val gainArea: Double = gain * area

        val preArea: Double = raster.tile.preArea.getData(col, row)
        val preAreaPlus: Double = if (row < 399) raster.tile.preArea.getData(col, row + 1) else raster.tile.preArea.getData(col, row)
        val preAreaMinus: Double = if (row > 0) raster.tile.preArea.getData(col, row - 1) else raster.tile.preArea.getData(col, row)


        val pKey = LossDataGroup(loss, tcd2000, tcd2010, drivers, globalLandCover, primaryForest, idnPrimaryForest, erosion,
          // biodiversitySignificance, biodiversityIntactness,
          wdpa, plantations, riverBasins, ecozones, urbanWatersheds,
          mangroves1996, mangroves2016, waterStress, intactForestLandscapes, endemicBirdAreas, tigerLandscapes,
          landmark, landRights, keyBiodiversityAreas, mining, rspo, peatlands, oilPalm, idnForestMoratorium,
          idnLandCover, mexProtectedAreas, mexPaymentForEcosystemServices, mexForestZoning, perProductionForest,
          perProtectedAreas, perForestConcessions, braBiomes, woodFiber, resourceRights, logging, oilGas)

        val summary: LossData = acc.stats.getOrElse(
          key = pKey,
          default = LossData(0, 0, 0, 0, 0, 0, 0, 0, 0, StreamingHistogram(size = 256), 0, 0, StreamingHistogram(size = 256)))

        summary.preArea += preArea
        summary.preAreaPlus += preAreaPlus
        summary.preAreaMinus += preAreaMinus

        summary.totalArea += area
        summary.totalAreaPlus += areaPlus
        summary.totalAreaMinus += areaMinus

        summary.totalGainArea += gainArea

        summary.totalBiomass += biomass * area / 10000
        summary.totalCo2 += ((biomass * area / 10000) * 0.5) * 44 / 12
        summary.biomassHistogram.countItem(biomass)

        summary.totalMangroveBiomass += mangroveBiomass * area / 10000
        summary.totalMangroveCo2 += ((mangroveBiomass * area / 10000) * 0.5) * 44 / 12
        summary.mangroveBiomassHistogram.countItem(mangroveBiomass)

        val updated_summary: Map[LossDataGroup, LossData] = acc.stats.updated(pKey, summary)

        TreeLossSummary(updated_summary)
      }
    }
}
