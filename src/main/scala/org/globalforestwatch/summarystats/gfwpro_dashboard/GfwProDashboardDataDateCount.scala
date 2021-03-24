package org.globalforestwatch.summarystats.gfwpro_dashboard

import io.circe.syntax._

import java.util.{Calendar, Date}
import scala.collection.immutable.SortedMap

case class GfwProDashboardDataDateCount(value: SortedMap[String, Int]) {
  def merge(
             other: GfwProDashboardDataDateCount
           ): GfwProDashboardDataDateCount = {

    GfwProDashboardDataDateCount(value ++ other.value.map {
      case (key, otherValue) =>
        key ->
          (value.getOrElse(key, 0) + otherValue)
    })
  }

  def toJson: String = {
    this.value.asJson.noSpaces
  }
}

object GfwProDashboardDataDateCount {
  def empty: GfwProDashboardDataDateCount =
    GfwProDashboardDataDateCount(SortedMap())

  def fill(alertDate: String,
           alertCount: Int,
           monthly: Boolean = false,
           viirs: Boolean = false): GfwProDashboardDataDateCount = {

    val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val alertDateCalendar = Calendar.getInstance();
    alertDateCalendar.setTime(formatter.parse(alertDate))

    val now = Calendar.getInstance()

    val (maxDate, minDate, alertDateFmt) = {
      if (monthly) {
        val dayOfMonth = now.get(Calendar.DAY_OF_MONTH)
        now.add(Calendar.DATE, -dayOfMonth)
        val maxDate = now.getTime

        now.add(Calendar.MONTH, -12)
        val minDate = now.getTime

        val alertDateFmt = alertDate.substring(0, 7)

        (maxDate, minDate, alertDateFmt)
      } else if (viirs) {
        val maxDate = now.getTime

        now.add(Calendar.DATE, -7)
        val minDate = now.getTime

        val alertDateFmt = alertDate

        (maxDate, minDate, alertDateFmt)
      } else {
        val maxDate = now.getTime

        now.add(Calendar.DATE, -now.get(Calendar.DAY_OF_WEEK))
        now.add(Calendar.WEEK_OF_YEAR, -3)
        val minDate = now.getTime

        val alertDateFmt = alertDate

        (maxDate, minDate, alertDateFmt)
      }
    }

    if (((alertDateCalendar.getTime before maxDate) || (alertDateCalendar.getTime equals maxDate)) &&
      (alertDateCalendar.getTime after minDate))
      GfwProDashboardDataDateCount(SortedMap(alertDateFmt -> alertCount))
    else this.empty
  }

}
