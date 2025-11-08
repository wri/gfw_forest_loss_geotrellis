package org.globalforestwatch.summarystats.integrated_alerts

import cats.implicits._
import geotrellis.raster.summary.GridVisitor
import geotrellis.raster._
import org.globalforestwatch.summarystats.Summary
import org.globalforestwatch.util.Geodesy

import java.time.LocalDate
import java.time.format.DateTimeFormatter

/** LossData Summary by year */
case class IntegratedAlertsSummary(stats: Map[IntegratedAlertsDataGroup, IntegratedAlertsData] = Map.empty)
  extends Summary[IntegratedAlertsSummary] {

  /** Combine two Maps and combine their LossData when a year is present in both */
  def merge(other: IntegratedAlertsSummary): IntegratedAlertsSummary = {
    // the years.combine method uses LossData.lossDataSemigroup instance to perform per value combine on the map
    IntegratedAlertsSummary(stats.combine(other.stats))
  }

  def isEmpty = stats.isEmpty
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

        // This is a pixel by pixel operation
        val gladL: Option[(LocalDate, Boolean)] =
          raster.tile.gladL.getData(col, row)
        val gladS2: Option[(LocalDate, Boolean)] =
          raster.tile.gladS2.getData(col, row)
        val radd: Option[(LocalDate, Boolean)] =
          raster.tile.radd.getData(col, row)
        val dist: Option[(LocalDate, Boolean)] =
          raster.tile.dist.getData(col, row)

        if (!(gladL.isEmpty && gladS2.isEmpty && radd.isEmpty)) {
          val biomass: Double = raster.tile.biomass.getData(col, row)
          val primaryForest: Boolean =
            raster.tile.primaryForest.getData(col, row)
          val protectedAreas: String =
            raster.tile.protectedAreas.getData(col, row)
          val landmark: Boolean = raster.tile.landmark.getData(col, row)
          val peatlands: Boolean = raster.tile.peatlands.getData(col, row)
          val mangroves2020: Boolean =
            raster.tile.mangroves2020.getData(col, row)
          val intactForestLandscapes2016: Boolean =
            raster.tile.intactForestLandscapes2016.getData(col, row)
          val naturalForests: String = raster.tile.naturalForests.getData(col, row)

          val re: RasterExtent = raster.rasterExtent
          val lat: Double = re.gridRowToMap(row)

          val area: Double = Geodesy.pixelArea(lat, re.cellSize) // uses Pixel's center coordinate.  +- raster.cellSize.height/2 doesn't make much of a difference

          val areaHa = area / 10000.0
          val co2Pixel = ((biomass * areaHa) * 0.5) * 44 / 12

          val gladLAlertDate: Option[String] = {
            gladL match {
              case Some((date, _)) => Some(date.format(DateTimeFormatter.ISO_DATE))
              case _ => None
            }
          }

          val gladLConfidence: Option[String] = {
            gladL match {
              case Some((_, conf)) =>
                if (conf) Some("high") else Some("low")
              case _ => None
            }
          }

          val gladS2AlertDate: Option[String] = {
            gladS2 match {
              case Some((date, _)) => Some(date.format(DateTimeFormatter.ISO_DATE))
              case _ => None
            }
          }

          val gladS2Confidence: Option[String] = {
            gladS2 match {
              case Some((_, conf)) =>
                if (conf) Some("high") else Some("nominal")
              case _ => None
            }
          }

          val raddAlertDate: Option[String] = {
            radd match {
              case Some((date, _)) => Some(date.format(DateTimeFormatter.ISO_DATE))
              case _ => None
            }
          }

          val raddConfidence: Option[String] = {
            radd match {
              case Some((_, conf)) =>
                if (conf) Some("high") else Some("nominal")
              case _ => None
            }
          }

          val distAlertDate: Option[String] = {
            dist match {
              case Some((date, _)) => Some(date.format(DateTimeFormatter.ISO_DATE))
              case _ => None
            }
          }

          val distConfidence: Option[String] = {
            dist match {
              case Some((_, conf)) =>
                if (conf) Some("high") else Some("nominal")
              case _ => None
            }
          }

          // remove Nones from date
          val alertDates: List[String] = List(gladLAlertDate, gladS2AlertDate, raddAlertDate).flatten

          // return min if any date exists
          val integratedAlertDate =
            if (alertDates.isEmpty) None
            else Some(alertDates.min)

          val confidences = List(gladLConfidence, gladS2Confidence, raddConfidence)
          val integratedConfidence = Some({
            // highest if more than one system detected alert
            if (confidences.flatten.size > 1) {
              "highest"
            } else if (confidences.contains(Some("high"))) {
              "high"
            } else {
              "nominal"
            }
          })

          var (intDistAlertDate, intDistConfidence): (Option[String], Option[String]) = 
            if (integratedAlertDate.isDefined && distAlertDate.isDefined) {
              // Alerts are defined for both integrated and dist alerts. If one alert
              // is 180 days newer than the other, take that newest date/confidence
              // (assuming this is a distinct new disturbance.) Otherwise, take the
              // older date, but with a "high" confidence, since at least two alert
              // systems show an alert.
              if (integratedAlertDate.get > distAlertDate.get + 180) {
                (integratedAlertDate, integratedConfidence)
              } else if (distAlertDate.get > integratedAlertDate.get + 180) {
                (distAlertDate, distConfidence)
              } else {
                (List(distAlertDate, integratedAlertDate).min, Some("highest"))
              }
            } else if (integratedAlertDate.isDefined) {
              (integratedAlertDate, integratedConfidence)
            } else if (distAlertDate.isDefined) {
              (distAlertDate, distConfidence)
            } else {
              (None, None)
            } 


          def updateSummary(
             stats: Map[IntegratedAlertsDataGroup, IntegratedAlertsData],
             integratedAlertDate: Option[String] = None,
             integratedConfidence: Option[String] = None,
             raddAlertDate: Option[String] = None,
             raddConfidence: Option[String] = None,
             gladS2AlertDate: Option[String]  = None,
             gladS2Confidence: Option[String] = None,
           ): Map[IntegratedAlertsDataGroup, IntegratedAlertsData] = {
              val pKey =
                IntegratedAlertsDataGroup(
                  //gladLAlertDate,
                  gladS2AlertDate,
                  raddAlertDate,
                  //gladLConfidence,
                  gladS2Confidence,
                  raddConfidence,
                  integratedAlertDate,
                  integratedConfidence,
                  intDistAlertDate,
                  intDistConfidence,
                  primaryForest,
                  protectedAreas,
                  landmark,
                  peatlands,
                  mangroves2020,
                  intactForestLandscapes2016,
                  naturalForests
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

          val updatedSummaryInt =
            updateSummary(acc.stats, integratedAlertDate = integratedAlertDate, integratedConfidence = integratedConfidence)

          val updatedSummaryRadd = raddAlertDate match {
            case Some(_) => updateSummary(updatedSummaryInt, raddAlertDate = raddAlertDate, raddConfidence = raddConfidence)
            case None => updatedSummaryInt
          }

          val updatedSummaryS2 = gladS2AlertDate match {
            case Some(_) => updateSummary(updatedSummaryRadd, gladS2AlertDate = gladS2AlertDate, gladS2Confidence = gladS2Confidence)
            case None => updatedSummaryRadd
          }

          acc = IntegratedAlertsSummary(updatedSummaryS2)
        }
      }
    }
  }
}
