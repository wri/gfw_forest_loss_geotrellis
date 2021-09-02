package org.globalforestwatch.summarystats.integrated_alerts

import cats.implicits._
import geotrellis.raster.summary.GridVisitor
import geotrellis.raster._
import org.globalforestwatch.summarystats.Summary
import org.globalforestwatch.util.Util.getAnyMapValue
import org.globalforestwatch.util.{Geodesy, Mercantile}

import scala.annotation.tailrec

/** LossData Summary by year */
case class IntegratedAlertsSummary(stats: Map[IntegratedAlertsDataGroup, IntegratedAlertsData] = Map.empty)
  extends Summary[IntegratedAlertsSummary] {

  /** Combine two Maps and combine their LossData when a year is present in both */
  def merge(other: IntegratedAlertsSummary): IntegratedAlertsSummary = {
    // the years.combine method uses LossData.lossDataSemigroup instance to perform per value combine on the map
    IntegratedAlertsSummary(stats.combine(other.stats))
  }
}

object IntegratedAlertsSummary {
  // TreeLossSummary form Raster[TreeLossTile] -- cell types may not be the same
  def getGridVisitor(kwargs: Map[String, Any]): GridVisitor[Raster[IntegratedAlertsTile], IntegratedAlertsSummary] = {
    new GridVisitor[Raster[IntegratedAlertsTile], IntegratedAlertsSummary] {
      private var acc: IntegratedAlertsSummary = new IntegratedAlertsSummary()

      def result: IntegratedAlertsSummary = acc

      def visit(raster: Raster[IntegratedAlertsTile],
                col: Int,
                row: Int): Unit = {

        val changeOnly: Boolean = getAnyMapValue[Boolean](kwargs, "changeOnly")

        // This is a pixel by pixel operation
        val gladL: Option[(String, String)] =
          raster.tile.gladL.getData(col, row)
        val gladS2: Option[(String, String)] =
          raster.tile.gladS2.getData(col, row)
        val radd: Option[(String, String)] =
          raster.tile.radd.getData(col, row)

        if (!(changeOnly && gladL.isEmpty && gladS2.isEmpty && radd.isEmpty)) {
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

          val lat: Double = raster.rasterExtent.gridRowToMap(row)

          val area: Double = Geodesy.pixelArea(lat, raster.cellSize) // uses Pixel's center coordinate.  +- raster.cellSize.height/2 doesn't make much of a difference

          val areaHa = area / 10000.0
          val co2Pixel = ((biomass * areaHa) * 0.5) * 44 / 12

          val gladLAlertDate: Option[String] = {
            gladL match {
              case Some((date, _)) => Some(date)
              case _ => None
            }
          }

          val gladLConfidence: String = {
            gladL match {
              case Some((_, conf)) => conf
              case _ => "not_detected"
            }
          }

          val gladS2AlertDate: Option[String] = {
            gladS2 match {
              case Some((date, _)) => Some(date)
              case _ => None
            }
          }

          val gladS2Confidence: String = {
            gladS2 match {
              case Some((_, conf)) => conf
              case _ => "not_detected"
            }
          }

          val raddAlertDate: Option[String] = {
            radd match {
              case Some((date, _)) => Some(date)
              case _ => None
            }
          }

          val raddConfidence: String = {
            radd match {
              case Some((_, conf)) => conf
              case _ => "not_detected"
            }
          }

          // remove Nones from date
          val alertDates: List[String] = List(gladLAlertDate, gladS2AlertDate, raddAlertDate).flatten

          // return min if any date exists
          val integratedAlertDate =
            if (alertDates.isEmpty) None
            else Some(alertDates.min)

          val confidences = List(gladLConfidence, gladS2Confidence, raddConfidence)
          val integratedConfidence = {
            // highest if more than one system detected alert
            if (confidences.filter(_ != "not_detected").size > 1) {
              "highest"
            } else if (confidences.contains("high")) {
              "high"
            } else if (confidences.contains("nominal")) {
              "nominal"
            } else {
              "not_detected"
            }
          }

          def updateSummary(
             stats: Map[IntegratedAlertsDataGroup, IntegratedAlertsData]
           ): Map[IntegratedAlertsDataGroup, IntegratedAlertsData] = {
              val pKey =
                IntegratedAlertsDataGroup(
                  gladLAlertDate,
                  gladS2AlertDate,
                  raddAlertDate,
                  gladLConfidence,
                  gladS2Confidence,
                  raddConfidence,
                  integratedAlertDate,
                  integratedConfidence,
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

              val summary: IntegratedAlertsData =
                stats.getOrElse(
                  key = pKey,
                  default = IntegratedAlertsData(0, 0, 0, 0)
                )

              summary.totalArea += areaHa

              if (gladL != null || gladS2 != null || radd != null) {
                summary.totalAlerts += 1
                summary.alertArea += areaHa
                summary.co2Emissions += co2Pixel
              }

              stats.updated(pKey, summary)
            }

          val updatedSummary: Map[IntegratedAlertsDataGroup, IntegratedAlertsData] =
            updateSummary(acc.stats)

          acc = IntegratedAlertsSummary(updatedSummary)
        }
      }
    }
  }
}
