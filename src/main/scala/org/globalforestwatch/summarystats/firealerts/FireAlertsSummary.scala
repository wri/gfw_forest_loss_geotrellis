package org.globalforestwatch.summarystats.firealerts

import cats.implicits._
import geotrellis.raster._
import geotrellis.raster.summary.GridVisitor
import org.globalforestwatch.summarystats.Summary
import org.globalforestwatch.util.Util.getAnyMapValue
import org.globalforestwatch.util.Geodesy

/** LossData Summary by year */
case class FireAlertsSummary(stats: Map[FireAlertsDataGroup, FireAlertsData] =
                             Map.empty)
  extends Summary[FireAlertsSummary] {

  /** Combine two Maps and combine their LossData when a year is present in both */
  def merge(other: FireAlertsSummary): FireAlertsSummary = {
    // the years.combine method uses LossData.lossDataSemigroup instance to perform per value combine on the map
    FireAlertsSummary(stats.combine(other.stats))
  }
  def isEmpty = stats.isEmpty
}

object FireAlertsSummary {
  // TreeLossSummary form Raster[TreeLossTile] -- cell types may not be the same

  def getGridVisitor(kwargs: Map[String, Any]) : GridVisitor[Raster[FireAlertsTile], FireAlertsSummary] = {
      new GridVisitor[Raster[FireAlertsTile], FireAlertsSummary] {
        private var acc: FireAlertsSummary = new FireAlertsSummary()

        def result: FireAlertsSummary = acc

        def visit(raster: Raster[FireAlertsTile],
                     col: Int,
                     row: Int): Unit = {
          val fireAlertType = getAnyMapValue[String](kwargs, "fireAlertType")

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
          val mangrovesLatest: Boolean =
            raster.tile.mangrovesLatest.getData(col, row)
          val intactForestLandscapes2016: Boolean =
            raster.tile.intactForestLandscapes2016.getData(col, row)
          val braBiomes: String = raster.tile.brazilBiomes.getData(col, row)
          val tcd2000: Integer = raster.tile.tcd2000.getData(col, row)

          val lat: Double = raster.rasterExtent.gridRowToMap(row)
          val area: Double = Geodesy.pixelArea(lat, raster.cellSize) // uses Pixel's center coordiate.  +- raster.cellSize.height/2 doesn't make much of a difference
          val areaHa = area / 10000.0

          val pKey =
            FireAlertsDataGroup(
              tcd2000,
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
              mangrovesLatest,
              intactForestLandscapes2016,
              braBiomes,
            )

          val summary: FireAlertsData =
            acc.stats.getOrElse(
              key = pKey,
              default = FireAlertsData(0)
            )

          fireAlertType match {
            case "modis" | "viirs" =>
              summary.total += 1
            case "burned_areas" =>
              summary.total += areaHa
          }

          acc = FireAlertsSummary(acc.stats.updated(pKey, summary))
        }
      }
    }
}
