package org.globalforestwatch.summarystats.gladalerts

import cats.implicits._
import geotrellis.raster.summary.GridVisitor
import geotrellis.raster._
import org.globalforestwatch.summarystats.Summary
import org.globalforestwatch.util.Util.getAnyMapValue
import org.globalforestwatch.util.{Geodesy, Mercantile}

import scala.annotation.tailrec
import java.time.format.DateTimeFormatter
import java.time.LocalDate

/** LossData Summary by year */
case class GladAlertsSummary(stats: Map[GladAlertsDataGroup, GladAlertsData] = Map.empty)
  extends Summary[GladAlertsSummary] {

  /** Combine two Maps and combine their LossData when a year is present in both */
  def merge(other: GladAlertsSummary): GladAlertsSummary = {
    // the years.combine method uses LossData.lossDataSemigroup instance to perform per value combine on the map
    GladAlertsSummary(stats.combine(other.stats))
  }
  def isEmpty = stats.isEmpty
}

object GladAlertsSummary {
  // TreeLossSummary form Raster[TreeLossTile] -- cell types may not be the same
  def getGridVisitor(kwargs: Map[String, Any]): GridVisitor[Raster[GladAlertsTile], GladAlertsSummary] = {
    new GridVisitor[Raster[GladAlertsTile], GladAlertsSummary] {
      private var acc: GladAlertsSummary = new GladAlertsSummary()

      def result: GladAlertsSummary = acc

      def visit(raster: Raster[GladAlertsTile],
                col: Int,
                row: Int): Unit = {

        val changeOnly: Boolean = getAnyMapValue[Boolean](kwargs, "changeOnly")

        val buildDataCube: Boolean = false //getAnyMapValue[Boolean](kwargs, "buildDataCube")

        val maxZoom = 12

        // This is a pixel by pixel operation
        val glad: Option[(LocalDate, Boolean)] =
          raster.tile.glad.getData(col, row)

        if (!(changeOnly && glad.isEmpty)) {
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
          val plantedForests: String = raster.tile.plantedForests.getData(col, row)
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
          val mangroves2020: Boolean =
            raster.tile.mangroves2020.getData(col, row)
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

          @tailrec
          def updateSummary(
                             tile: Mercantile.Tile,
                             stats: Map[GladAlertsDataGroup, GladAlertsData]
                           ): Map[GladAlertsDataGroup, GladAlertsData] = {
            val cutOff: Int = {
              if (buildDataCube) 0
              else maxZoom
            }

            if (tile.z < cutOff) stats
            else {
              val alertDate: String = {
                glad match {
                  case Some((date, _)) => date.format(DateTimeFormatter.ISO_DATE)
                  case _ => null
                }
              }

              val confidence: String = {
                glad match {
                  case Some((_, conf)) =>
                    if (conf) "high" else "low"
                  case _ => "not_detected"
                }
              }

              val pKey =
                GladAlertsDataGroup(
                  alertDate,
                  (confidence == "high"),
                  tile,
                  climateMask,
                  primaryForest,
                  protectedAreas,
                  aze,
                  keyBiodiversityAreas,
                  landmark,
                  plantedForests,
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
                  mangroves2020,
                  intactForestLandscapes2016,
                  braBiomes
                )

              val summary: GladAlertsData =
                stats.getOrElse(
                  key = pKey,
                  default = GladAlertsData(0, 0, 0, 0)
                )

              summary.totalArea += areaHa

              if (glad != null) {
                summary.totalAlerts += 1
                summary.alertArea += areaHa
                summary.co2Emissions += co2Pixel
              }

              updateSummary(
                Mercantile.parent(tile),
                stats.updated(pKey, summary)
              )
            }

          }

          val tile: Mercantile.Tile = Mercantile.tile(lng, lat, maxZoom)
          val updatedSummary: Map[GladAlertsDataGroup, GladAlertsData] =
            updateSummary(tile, acc.stats)

          acc = GladAlertsSummary(updatedSummary)
        }
      }
    }
  }
}
