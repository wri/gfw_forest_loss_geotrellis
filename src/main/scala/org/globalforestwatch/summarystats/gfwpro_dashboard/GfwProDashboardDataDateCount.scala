package org.globalforestwatch.summarystats.gfwpro_dashboard

import io.circe.syntax._

import scala.collection.immutable.SortedMap
import frameless.Injection
import cats.implicits._
import java.time.LocalDate
import java.time.format._
import java.time.temporal._

case class GfwProDashboardDataDateCount(value: SortedMap[String, Int]) {

  def merge(other: GfwProDashboardDataDateCount): GfwProDashboardDataDateCount = {
    GfwProDashboardDataDateCount(value combine other.value)
  }

  def toJson: String = this.value.asJson.noSpaces
}

object GfwProDashboardDataDateCount {

  /** ex: 2016-1-1 => 2015-53 because the 1st of 2016 is Friday of the last week of 2015 */
  val WeekOfYear =
    new DateTimeFormatterBuilder()
      .parseCaseInsensitive()
      .appendValue(IsoFields.WEEK_BASED_YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
      .appendLiteral("-")
      .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 2)
      .toFormatter(java.util.Locale.US);

  val MonthOfYear =
    new DateTimeFormatterBuilder()
      .parseCaseInsensitive()
      .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
      .appendLiteral("-")
      .appendValue(ChronoField.MONTH_OF_YEAR, 2)
      .toFormatter(java.util.Locale.US);

  implicit def injection: Injection[GfwProDashboardDataDateCount, String] = Injection(_.toJson, fromString)

  def empty: GfwProDashboardDataDateCount = GfwProDashboardDataDateCount(SortedMap())

  /** Record alerts as a GfwProDashboardDataDateCount for the specified day if
    * alertDate is not None and include is true, else return
    * GfwProDashboardDataDateCount.empty. */
  def fillDaily(alertDate: Option[LocalDate], include: Boolean, alertCount: Int): GfwProDashboardDataDateCount =
    fill(if (include) alertDate else None, alertCount, _.format(DateTimeFormatter.ISO_DATE))

  /** Record alerts as a GfwProDashboardDataDateCount for the appropriate week if
    * alertDate is not None, else return GfwProDashboardDataDateCount.empty. */
  def fillWeekly(alertDate: Option[LocalDate], alertCount: Int): GfwProDashboardDataDateCount =
    fill(alertDate, alertCount, _.format(WeekOfYear))

  /** Record alerts as a GfwProDashboardDataDateCount for the appropriate month if
    * alertDate is not None, else return GfwProDashboardDataDateCount.empty. */
  def fillMonthly(alertDate: Option[LocalDate], alertCount: Int): GfwProDashboardDataDateCount =
    fill(alertDate, alertCount, _.format(MonthOfYear))

  def fill(
    alertDate: Option[LocalDate],
    alertCount: Int,
    formatter: LocalDate => String
  ): GfwProDashboardDataDateCount = {

    alertDate match {
      case Some(date) =>
        val dateKey: String = formatter(date)
        GfwProDashboardDataDateCount(SortedMap(dateKey -> alertCount))

      case _ =>
        this.empty
    }
  }

  def fromString(value: String): GfwProDashboardDataDateCount = {
    val sortedMap = io.circe.parser.decode[SortedMap[String, Int]](value)
    GfwProDashboardDataDateCount(sortedMap.getOrElse(SortedMap()))
  }
}
