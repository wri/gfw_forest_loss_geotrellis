package org.globalforestwatch.summarystats.firealerts

import cats.implicits._
import geotrellis.contrib.polygonal.CellVisitor
import geotrellis.raster._
import org.globalforestwatch.summarystats.Summary
import org.globalforestwatch.util.Util.getAnyMapValue
import org.globalforestwatch.util.{Geodesy, Mercantile}

/** LossData Summary by year */
case class FireAlertsSummary(stats: Map[FireAlertsDataGroup, FireAlertsData] =
                             Map.empty,
                             kwargs: Map[String, Any])
  extends Summary[FireAlertsSummary] {

  /** Combine two Maps and combine their LossData when a year is present in both */
  def merge(other: FireAlertsSummary): FireAlertsSummary = {
    // the years.combine method uses LossData.lossDataSemigroup instance to perform per value combine on the map
    FireAlertsSummary(stats.combine(other.stats), kwargs)
  }
}

object FireAlertsSummary {
  // TreeLossSummary form Raster[TreeLossTile] -- cell types may not be the same

  implicit val mdhCellRegisterForFireAlertsRaster
    : CellVisitor[Raster[FireAlertsTile], FireAlertsSummary] =
    new CellVisitor[Raster[FireAlertsTile], FireAlertsSummary] {

      def register(raster: Raster[FireAlertsTile],
                   col: Int,
                   row: Int,
                   acc: FireAlertsSummary): FireAlertsSummary = {

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

        def updateSummary(stats: Map[FireAlertsDataGroup, FireAlertsData]): Map[FireAlertsDataGroup, FireAlertsData] = {
            val pKey =
              FireAlertsDataGroup(
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

            val summary: FireAlertsData =
              stats.getOrElse(
                key = pKey,
                default = FireAlertsData(0)
              )

            summary.totalAlerts += 1

            stats.updated(pKey, summary)
          }

        val updatedSummary: Map[FireAlertsDataGroup, FireAlertsData] =
          updateSummary(acc.stats)

        FireAlertsSummary(updatedSummary, acc.kwargs)
    }
  }
}
