package org.globalforestwatch.summarystats.integrated_alerts

import cats.implicits._
import geotrellis.raster.summary.GridVisitor
import geotrellis.raster._
import org.globalforestwatch.summarystats.Summary
import org.globalforestwatch.util.Geodesy

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

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
  val twoYearsAgo = LocalDate.now().minus(2, ChronoUnit.YEARS)

  // TreeLossSummary form Raster[TreeLossTile] -- cell types may not be the same
  def getGridVisitor(kwargs: Map[String, Any]): GridVisitor[Raster[IntegratedAlertsTile], IntegratedAlertsSummary] = {
    new GridVisitor[Raster[IntegratedAlertsTile], IntegratedAlertsSummary] {
      private var acc: IntegratedAlertsSummary = new IntegratedAlertsSummary()

      def result: IntegratedAlertsSummary = acc

      def visit(raster: Raster[IntegratedAlertsTile],
                col: Int,
                row: Int): Unit = {

        // Drop any alerts older than two year ago.
        val (gladLAlertDate, gladLConfidence): (Option[LocalDate], Option[String]) = {
          val x = raster.tile.gladL.getData(col, row)
          x match {
            case Some((date, conf)) =>
              if (date.isAfter(twoYearsAgo))
                (Some(date), if (conf) Some("high") else Some("low"))
              else
                  (None, None)
            case _ => (None, None)
          }
        }
        val (gladS2AlertDate, gladS2Confidence): (Option[LocalDate], Option[String]) = {
          val x = raster.tile.gladS2.getData(col, row)
          x match {
            case Some((date, conf)) =>
              if (date.isAfter(twoYearsAgo))
                (Some(date), if (conf) Some("high") else Some("nominal"))
              else
                (None, None)
            case _ => (None, None)
          }
        }
        val (raddAlertDate, raddConfidence): (Option[LocalDate], Option[String]) = {
          val x = raster.tile.radd.getData(col, row)
          x match {
            case Some((date, conf)) =>
              if (date.isAfter(twoYearsAgo))
                (Some(date), if (conf) Some("high") else Some("nominal"))
              else
                (None, None)
            case _ => (None, None)
          }
        }
        val (distAlertDate, distConfidence): (Option[LocalDate], Option[String]) = {
          val x = raster.tile.dist.getData(col, row)
          x match {
            case Some((date, conf)) =>
              if (date.isAfter(twoYearsAgo))
                (Some(date), if (conf) Some("high") else Some("nominal"))
              else
                (None, None)
            case _ => (None, None)
          }
        }

        // Note: this check causes all remaining code to be skipped (if all input
        // alerts are NONE)!!
        if (!(gladLAlertDate.isEmpty && gladS2AlertDate.isEmpty && raddAlertDate.isEmpty && distAlertDate.isEmpty)) {
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
          val treeCover2022: Boolean = raster.tile.treeCover2022.getData(col, row)

          val re: RasterExtent = raster.rasterExtent
          val lat: Double = re.gridRowToMap(row)

          val area: Double = Geodesy.pixelArea(lat, re.cellSize) // uses Pixel's center coordinate.  +- raster.cellSize.height/2 doesn't make much of a difference

          val areaHa = area / 10000.0
          val co2Pixel = ((biomass * areaHa) * 0.5) * 44 / 12

          // remove Nones from list of dates
          val alertDates: List[LocalDate] = List(gladLAlertDate, gladS2AlertDate, raddAlertDate).flatten

          val (integratedAlertDate, integratedConfidence): (Option[LocalDate], Option[String]) = {
            if (alertDates.isEmpty) (None, None)
            else {
              // return min if any date exists
              val date = Some(alertDates.minBy(_.toEpochDay))
              val confidences = List(gladLConfidence, gladS2Confidence, raddConfidence).flatten
              val conf = Some({
                // highest if more than one system detected alert
                if (confidences.size > 1) {
                  "highest"
                } else if (confidences.contains("high")) {
                  "high"
                } else {
                  "nominal"
                }
              })
              (date, conf)
            }
          }


          val (intDistAlertDate, intDistConfidence): (Option[LocalDate], Option[String]) = 
            if (integratedAlertDate.isDefined && distAlertDate.isDefined) {
              val daysBetween = ChronoUnit.DAYS.between(distAlertDate.get, integratedAlertDate.get)
              // Alerts are defined for both integrated and dist alerts. If one alert
              // is 180 days newer than the other, take that newest date/confidence
              // (assuming this is a distinct new disturbance.) Otherwise, take the
              // older date, but with a "highest" confidence, since at least two
              // alert systems show an alert.
              if (daysBetween > 180) {
                (integratedAlertDate, integratedConfidence)
              } else if (daysBetween < -180) {
                (distAlertDate, distConfidence)
              } else {
                (Some(List(distAlertDate.get, integratedAlertDate.get).minBy(_.toEpochDay)), Some("highest"))
              }
            } else if (integratedAlertDate.isDefined) {
              (integratedAlertDate, integratedConfidence)
            } else {
              assert(distAlertDate.isDefined)
              (distAlertDate, distConfidence)
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
                  gladS2AlertDate,
                  raddAlertDate,
                  gladS2Confidence,
                  raddConfidence,
                  integratedAlertDate,
                  integratedConfidence,
                  primaryForest,
                  protectedAreas,
                  landmark,
                  peatlands,
                  mangroves2020,
                  intactForestLandscapes2016,
                  naturalForests,
                  treeCover2022
                )

              val summary: IntegratedAlertsData =
                stats.getOrElse(
                  key = pKey,
                  default = IntegratedAlertsData(0, 0, 0, 0)
                )

              // totalArea would not be accurate (since we skip all this code if the
              // pixel has no alerts), but it isn't put in the output anyway.
              //summary.totalArea += areaHa

              summary.totalAlerts += 1
              summary.alertArea += areaHa
              summary.co2Emissions += co2Pixel

              stats.updated(pKey, summary)
            }

          // For each pixel, record an IntegratedAlertsDataGroup (which will
          // aggregate to a separate row) with just int-dist alerts date/confidence,
          // which must be non-None, since at least one alert is non-None. Then
          // record separate IntegratedAlertsDataGroups for radd and gladS2 alerts,
          // if none-None. We do separate IntegratedAlertsDataGroups to avoid
          // multiplying the number of rows.
          val updatedSummaryInt =
            updateSummary(acc.stats, integratedAlertDate = Some(intDistAlertDate.get.format(DateTimeFormatter.ISO_DATE)), integratedConfidence = intDistConfidence)

          val updatedSummaryRadd = raddAlertDate match {
            case Some(_) => updateSummary(updatedSummaryInt, raddAlertDate = Some(raddAlertDate.get.format(DateTimeFormatter.ISO_DATE)), raddConfidence = raddConfidence)
            case None => updatedSummaryInt
          }

          val updatedSummaryS2 = gladS2AlertDate match {
            case Some(_) => updateSummary(updatedSummaryRadd, gladS2AlertDate = Some(gladS2AlertDate.get.format(DateTimeFormatter.ISO_DATE)), gladS2Confidence = gladS2Confidence)
            case None => updatedSummaryRadd
          }

          acc = IntegratedAlertsSummary(updatedSummaryS2)
        }
      }
    }
  }
}
