package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridId.toGladGridId
import org.globalforestwatch.grids.GridTile

import java.time.LocalDate
import java.time.format.DateTimeFormatter

case class RaddAlerts(gridTile: GridTile, kwargs: Map[String, Any])
  extends DateConfLayer
    with OptionalILayer {
  val datasetName = "wur_radd_alerts"
  val uri: String =
    s"$basePath/$datasetName/$version/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/date_conf/gdal-geotiff/${gridTile.tileId}.tif"

  override def lookup(value: Int): Option[(String, Boolean)] = {

    val confidence = value >= 30000
    val alertDate: Option[String] = {

      def isLeapYear(year: Int): Boolean = {
        implicit def int2boolRev(i: Int): Boolean = i <= 0

        year % 4
      }

      def getDateString(days: Int, year: Int): String = {
        val daysInYear = if (isLeapYear(year)) 366 else 365
        if (days <= daysInYear) s"$year" + "%03d".format(days)
        else getDateString(days - daysInYear, year + 1)
      }

      val julianDate = DateTimeFormatter.ofPattern("yyyyDDD")
      val days: Int = if (confidence) value - 30000 else value - 20000

      days match {
        case d if d < 0 => None
        case d if d == 0 =>
          Some(
            LocalDate
              .parse(getDateString(365, 2014), julianDate)
              .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
          )
        case _ =>
          Some(
            LocalDate
              .parse(getDateString(days, 2015), julianDate)
              .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
          )
      }

    }

    alertDate match {
      case Some(d) => Some(d, confidence)
      case None => None
    }

  }

}
