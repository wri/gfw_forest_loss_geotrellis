package org.globalforestwatch.layers

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.globalforestwatch.grids.GridId.toGladGridId

case class GladAlerts(grid: String) extends DateConfLayer with RequiredILayer {

  val gladGrid: String = toGladGridId(grid)

  val uri: String =
    s"s3://gfw2-data/forest_change/umd_landsat_alerts/prod/analysis/$gladGrid.tif"

  override def lookup(value: Int): Option[(String, Boolean)] = {

    val confidence = value >= 30000
    val alertDate: Option[String] = {

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

      if (days < 0) None
      else if (days == 0)
        Some(
          LocalDate
            .parse(getDateString(365, 2014), julianDate)
            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
        )
      else
        Some(
          LocalDate
            .parse(getDateString(days, 2015), julianDate)
            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
        )

    }

    alertDate match {
      case Some(d) => Some(d, confidence)
      case None => null
    }

  }

}
