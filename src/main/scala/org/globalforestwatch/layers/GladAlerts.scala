package org.globalforestwatch.layers

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.globalforestwatch.grids.GridId.toGladGridId

case class GladAlerts(grid: String) extends DateConfLayer with RequiredILayer {

  val gladGrid: String = toGladGridId(grid)

  val uri: String =
    s"s3://gfw2-data/forest_change/umd_landsat_alerts/prod/analysis/$gladGrid.tif"

  override def lookup(value: Int): (LocalDate, Boolean) = {

    val confidence = value >= 30000
    val alertDate: LocalDate = {

      def isLeapYear(year: Int): Boolean = {
        implicit def int2boolRev(i: Int): Boolean = if (i > 0) false else true
        year % 4
      }

      def getDateString(days: Int, year: Int): String = {
        val daysInYear = if (isLeapYear(year)) 366 else 365
        if (days <= daysInYear) s"$year" + "%03d".format(days)
        else getDateString(days - daysInYear, year + 1)
      }

      val julianDate = DateTimeFormatter.ofPattern("yyyyDDD")
      val days: Int = if (confidence) value - 30000 else value - 20000

      if (days < 0) null
      else if (days == 0) LocalDate.parse(getDateString(365, 2014), julianDate)
      else LocalDate.parse(getDateString(days, 2015), julianDate)

    }

    (alertDate, confidence)
  }

}
