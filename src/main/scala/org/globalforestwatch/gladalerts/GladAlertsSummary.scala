package org.globalforestwatch.gladalerts

import java.time.LocalDate
import geotrellis.contrib.polygonal.CellVisitor
import geotrellis.raster._
import cats.implicits._
import org.globalforestwatch.util.Geodesy
import org.globalforestwatch.util.Mercantile

/** LossData Summary by year */
case class GladAlertsSummary(
  stats: Map[GladAlertsDataGroup, GladAlertsData] = Map.empty
) {

  /** Combine two Maps and combine their LossData when a year is present in both */
  def merge(other: GladAlertsSummary): GladAlertsSummary = {
    // the years.combine method uses LossData.lossDataSemigroup instance to perform per value combine on the map
    GladAlertsSummary(stats.combine(other.stats))
  }
}

object GladAlertsSummary {
  // TreeLossSummary form Raster[TreeLossTile] -- cell types may not be the same

  implicit val mdhCellRegisterForTreeLossRaster1
    : CellVisitor[Raster[GladAlertsTile], GladAlertsSummary] =
    new CellVisitor[Raster[GladAlertsTile], GladAlertsSummary] {

      def register(raster: Raster[GladAlertsTile],
                   col: Int,
                   row: Int,
                   acc: GladAlertsSummary): GladAlertsSummary = {
        // This is a pixel by pixel operation
        val glad: (LocalDate, Boolean) = raster.tile.glad.getData(col, row)

        if (glad != null) {
          val biomass: Double = raster.tile.biomass.getData(col, row)
          val climateMask: Boolean = raster.tile.climateMask.getData(col, row)
          val primaryForest: Boolean =
            raster.tile.primaryForest.getData(col, row)
          val protectedAreas: String =
            raster.tile.protectedAreas.getData(col, row)
          val aze: Boolean = raster.tile.aze.getData(col, row)
          val keyBiodiversityAreas: Boolean =
            raster.tile.keyBiodiversityAreas.getData(col, row)
          val landmark: Boolean = raster.tile.landmark.getData(col, row)
          val plantations: String = raster.tile.plantations.getData(col, row)
          val mining: Boolean = raster.tile.mining.getData(col, row)
          val logging: Boolean = raster.tile.logging.getData(col, row)
          val rspo: String = raster.tile.rspo.getData(col, row)
          val woodFiber: Boolean = raster.tile.woodFiber.getData(col, row)
          val peatlands: Boolean = raster.tile.peatlands.getData(col, row)
          val indonesiaForestMoratorium: Boolean =
            raster.tile.indonesiaForestMoratorium.getData(col, row)
          val oilPalm: Boolean = raster.tile.oilPalm.getData(col, row)
          val indonesiaForestArea: String =
            raster.tile.indonesiaForestArea.getData(col, row)
          val peruForestConcessions: String =
            raster.tile.peruForestConcessions.getData(col, row)
          val oilGas: Boolean = raster.tile.oilGas.getData(col, row)
          val mangroves2016: Boolean =
            raster.tile.mangroves2016.getData(col, row)
          val intactForestLandscapes2016: Boolean =
            raster.tile.intactForestLandscapes2016.getData(col, row)
          val braBiomes: String = raster.tile.brazilBiomes.getData(col, row)

          val cols: Int = raster.rasterExtent.cols
          val rows: Int = raster.rasterExtent.rows
          val ext = raster.rasterExtent.extent
          val cellSize = raster.cellSize

          val lat: Double = raster.rasterExtent.gridRowToMap(row)
          val lng: Double = raster.rasterExtent.gridColToMap(col)

          val area: Double = Geodesy.pixelArea(lat, raster.cellSize) // uses Pixel's center coordinate.  +- raster.cellSize.height/2 doesn't make much of a difference

          val areaHa = area / 10000.0
          val co2Pixel = ((biomass * areaHa) * 0.5) * 44 / 12

          def updateSummary(
                             tile: Mercantile.Tile,
                             stats: Map[GladAlertsDataGroup, GladAlertsData]
                           ): Map[GladAlertsDataGroup, GladAlertsData] = {
            if (tile.z < 0) stats
            else {
              val pKey =
                GladAlertsDataGroup(
                  glad._1,
                  glad._2,
                  tile,
                  climateMask,
                  primaryForest,
                  protectedAreas,
                  aze,
                  keyBiodiversityAreas,
                  landmark,
                  plantations,
                  mining,
                  logging,
                  rspo,
                  woodFiber,
                  peatlands,
                  indonesiaForestMoratorium,
                  oilPalm,
                  indonesiaForestArea,
                  peruForestConcessions,
                  oilGas,
                  mangroves2016,
                  intactForestLandscapes2016,
                  braBiomes
                )

              val summary: GladAlertsData =
                stats.getOrElse(key = pKey, default = GladAlertsData(0, 0, 0))

              summary.totalAlerts += 1
              summary.totalArea += areaHa
              summary.totalCo2 += co2Pixel
              updateSummary(
                Mercantile.parent(tile),
                stats.updated(pKey, summary)
              )
            }

          }

          val tile: Mercantile.Tile = Mercantile.tile(lng, lat, 12)
          val updatedSummary: Map[GladAlertsDataGroup, GladAlertsData] =
            updateSummary(tile, acc.stats)

          GladAlertsSummary(updatedSummary)

        } else {
          acc
        }
      }
    }
}