package org.globalforestwatch.summarystats.afi

import io.circe.syntax._

import scala.collection.immutable.SortedMap
import frameless.Injection
import cats.implicits._
import java.time.LocalDate
import java.time.format._
import java.time.temporal._

case class AFiDataDateCount(value: SortedMap[String, Int]) {

  def merge(other: AFiDataDateCount): AFiDataDateCount = {
    AFiDataDateCount(value combine other.value)
  }

  def toJson: String = this.value.asJson.noSpaces
}

object AFiDataDateCount {

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

  implicit def injection: Injection[AFiDataDateCount, String] = Injection(_.toJson, fromString)

  def empty: AFiDataDateCount = AFiDataDateCount(SortedMap())

  def fillDaily(alertDate: Option[LocalDate], alertCount: Int): AFiDataDateCount =
    fill(alertDate, alertCount, _.format(DateTimeFormatter.ISO_DATE))

  def fillWeekly(alertDate: Option[LocalDate], alertCount: Int): AFiDataDateCount =
    fill(alertDate, alertCount, _.format(WeekOfYear))

  def fillMonthly(alertDate: Option[LocalDate], alertCount: Int): AFiDataDateCount =
    fill(alertDate, alertCount, _.format(MonthOfYear))

  def fill(
    alertDate: Option[LocalDate],
    alertCount: Int,
    formatter: LocalDate => String
  ): AFiDataDateCount = {

    alertDate match {
      case Some(date) =>
        val dateKey: String = formatter(date)
        AFiDataDateCount(SortedMap(dateKey -> alertCount))

      case _ =>
        this.empty
    }
  }

  def fromString(value: String): AFiDataDateCount = {
    val sortedMap = io.circe.parser.decode[SortedMap[String, Int]](value)
    AFiDataDateCount(sortedMap.getOrElse(SortedMap()))
  }
}
