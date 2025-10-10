package org.globalforestwatch.summarystats.gfwpro_dashboard

import io.circe.syntax._

import scala.collection.immutable.SortedMap
import frameless.Injection
import cats.implicits._
import java.time.LocalDate
import java.time.format._
import java.time.temporal._

case class GfwProDashboardDataDateArea(value: SortedMap[String, Double]) {

  def merge(other: GfwProDashboardDataDateArea): GfwProDashboardDataDateArea = {
    GfwProDashboardDataDateArea(value combine other.value)
  }

  def roundDouble(value: Double, digits: Int = 4): Double = {
    Math.round(value * math.pow(10, digits)) / math.pow(10, digits)
  }

  def round: SortedMap[String, Double] ={
    val a = this.value
    this.value.map {
      case (key, value) =>
        key -> roundDouble(value)
    }
  }

  def toJson: String = this.round.asJson.noSpaces
}

object GfwProDashboardDataDateArea {

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

  implicit def injection: Injection[GfwProDashboardDataDateArea, String] = Injection(_.toJson, fromString)

  def empty: GfwProDashboardDataDateArea = GfwProDashboardDataDateArea(SortedMap())

  /** Record alerts as a GfwProDashboardDataDateArea for the specified day if
    * alertDate is not None and include is true, else return
    * GfwProDashboardDataDateArea.empty. */
  def fillDaily(alertDate: Option[LocalDate], include: Boolean, alertArea: Double): GfwProDashboardDataDateArea =
    fill(if (include) alertDate else None, alertArea, _.format(DateTimeFormatter.ISO_DATE))

  /** Record alerts as a GfwProDashboardDataDateArea for the appropriate week if
    * alertDate is not None, else return GfwProDashboardDataDateArea.empty. */
  def fillWeekly(alertDate: Option[LocalDate], alertArea: Double): GfwProDashboardDataDateArea =
    fill(alertDate, alertArea, _.format(WeekOfYear))

  /** Record alerts as a GfwProDashboardDataDateArea for the appropriate month if
    * alertDate is not None, else return GfwProDashboardDataDateArea.empty. */
  def fillMonthly(alertDate: Option[LocalDate], alertArea: Double): GfwProDashboardDataDateArea =
    fill(alertDate, alertArea, _.format(MonthOfYear))

  def fill(
    alertDate: Option[LocalDate],
    alertArea: Double,
    formatter: LocalDate => String
  ): GfwProDashboardDataDateArea = {

    alertDate match {
      case Some(date) =>
        val dateKey: String = formatter(date)
        GfwProDashboardDataDateArea(SortedMap(dateKey -> alertArea))

      case _ =>
        this.empty
    }
  }

  def fromString(value: String): GfwProDashboardDataDateArea = {
    val sortedMap = io.circe.parser.decode[SortedMap[String, Double]](value)
    GfwProDashboardDataDateArea(sortedMap.getOrElse(SortedMap()))
  }
}
