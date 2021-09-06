package org.globalforestwatch.summarystats.gfwpro_dashboard

import io.circe.syntax._

import java.util.Calendar
import scala.collection.immutable.SortedMap
import frameless.Injection

case class GfwProDashboardDataDateCount(value: SortedMap[String, Int]) {

  def merge(other: GfwProDashboardDataDateCount): GfwProDashboardDataDateCount = {
    GfwProDashboardDataDateCount(value ++ other.value.map {
      case (key, otherValue) =>
        key -> (value.getOrElse(key, 0) + otherValue)
    })
  }

  def toJson: String = this.value.asJson.noSpaces
}

object GfwProDashboardDataDateCount {
  implicit def injection: Injection[GfwProDashboardDataDateCount, String] = Injection(_.toJson, fromString)

  def empty: GfwProDashboardDataDateCount =
    GfwProDashboardDataDateCount(SortedMap())

  def fill(alertDate: Option[String],
           alertCount: Int,
           weekly: Boolean = false,
           monthly: Boolean = false,
           viirs: Boolean = false): GfwProDashboardDataDateCount = {

    alertDate match {
      case Some(date) => {

        val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd")
        val alertDateCalendar = Calendar.getInstance();
        alertDateCalendar.setTime(formatter.parse(date))

        val now = Calendar.getInstance()

        val (maxDate, minDate, alertDateFmt) = {
          if (monthly) {
            val dayOfMonth = now.get(Calendar.DAY_OF_MONTH)
            now.add(Calendar.DATE, -dayOfMonth)
            val maxDate = now.getTime

            now.add(Calendar.MONTH, -12)
            val minDate = now.getTime

            val alertDateFmt =
              f"${alertDateCalendar.get(Calendar.YEAR)}-${alertDateCalendar.get(Calendar.MONTH)}%02d"

            (maxDate, minDate, alertDateFmt)
          } else if (weekly) {
            val dayOfWeek = now.get(Calendar.DAY_OF_WEEK)
            now.add(Calendar.DATE, -dayOfWeek)
            val maxDate = now.getTime

            now.add(Calendar.WEEK_OF_YEAR, -4)
            val minDate = now.getTime

            val alertDateFmt =
              f"${alertDateCalendar.get(Calendar.YEAR)}-${alertDateCalendar.get(Calendar.WEEK_OF_YEAR)}%02d"

            (maxDate, minDate, alertDateFmt)
          } else if (viirs) {
            val maxDate = now.getTime

            now.add(Calendar.DATE, -7)
            val minDate = now.getTime

            val alertDateFmt = date

            (maxDate, minDate, alertDateFmt)
          } else {
            val maxDate = now.getTime

            now.add(Calendar.DATE, -now.get(Calendar.DAY_OF_WEEK))
            now.add(Calendar.WEEK_OF_YEAR, -4)
            val minDate = now.getTime

            val alertDateFmt = date

            (maxDate, minDate, alertDateFmt)
          }
        }

        if (((alertDateCalendar.getTime before maxDate) || (alertDateCalendar.getTime equals maxDate)) &&
          (alertDateCalendar.getTime after minDate))
          GfwProDashboardDataDateCount(SortedMap(alertDateFmt -> alertCount))
        else this.empty
      }

      case _ => this.empty
    }
  }

  def fromString(value: String): GfwProDashboardDataDateCount = {
    val sortedMap = io.circe.parser.decode[SortedMap[String, Int]](value)
    GfwProDashboardDataDateCount(sortedMap.getOrElse(SortedMap()))
  }
}
