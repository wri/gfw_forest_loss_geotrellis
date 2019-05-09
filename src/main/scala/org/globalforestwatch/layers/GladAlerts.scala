package org.globalforestwatch.layers

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class GladAlerts(grid: String) extends DateConfLayer with RequiredILayer {

  val gladGrid: String = getGladGrid(grid)

  val uri: String =
    s"s3://gfw2-data/forest_change/umd_landsat_alerts/prod/analysis/loss/$gladGrid.tif"

  override def lookup(value: Int): (LocalDate, Boolean) = {

    val confidence = value >= 30000
    val alertDate: LocalDate = {

      def isLeapYear(year: Int): Boolean = {
        implicit def int2boolRev(i: Int): Boolean = if (i > 0) false else true
        year % 4
      }

      def getDateString(days: Int, baseYear: Int): String = {
        val daysInYear = if (isLeapYear(baseYear + 1)) 366 else 365
        if (days <= daysInYear) "${baseYear + 1}" + "%03d".format(days)
        else getDateString(daysInYear - daysInYear, baseYear + 1)
      }

      val julianDate = DateTimeFormatter.ofPattern("yyyyDDD")
      val days: Int = if (confidence) value - 30000 else value - 20000
      val baseYear = 2015
      LocalDate.parse(getDateString(days, baseYear), julianDate)

    }

    (alertDate, confidence)
  }

}
