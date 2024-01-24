package org.globalforestwatch.summarystats.forest_change_diagnostic

import frameless.Injection

import scala.collection.immutable.SortedMap
import io.circe.syntax._
import io.circe.parser.decode
import cats.kernel.Semigroup
import cats.implicits._

case class ForestChangeDiagnosticDataValueYearly(value: SortedMap[Int, Double])
  extends ForestChangeDiagnosticDataParser[ForestChangeDiagnosticDataValueYearly] {

  def merge(other: ForestChangeDiagnosticDataValueYearly): ForestChangeDiagnosticDataValueYearly = {
    ForestChangeDiagnosticDataValueYearly(Semigroup[SortedMap[Int, Double]].combine(value, other.value))
  }

  def limitToMaxYear(maxYear: Int): ForestChangeDiagnosticDataValueYearly= {
    ForestChangeDiagnosticDataValueYearly(value.filterKeys{ year => year <= maxYear })
  }

  def toJson: String = {
    this.round.asJson.noSpaces
  }

  def round: SortedMap[Int, Double] = this.value.map {
    case (key, value) => key -> this.round(value)
  }
}

object ForestChangeDiagnosticDataValueYearly {
  def fill(
            baseValue: Double,
            diff: SortedMap[Int, Double] =
            ForestChangeDiagnosticDataValueYearly.prefilled.value,
            shift: Int = 0
          ): ForestChangeDiagnosticDataValueYearly = {

    // Only except lossYear values within range of default map or 0
    val minExtentYear: Int = this.prefilled.value.keysIterator.min
    val maxExtentYear: Int = this.prefilled.value.keysIterator.max
    val years: List[Int] = List.range(minExtentYear, maxExtentYear + 1)

    ForestChangeDiagnosticDataValueYearly.prefilled.merge(
      ForestChangeDiagnosticDataValueYearly(SortedMap(years.map(year => {

        val diffYears: List[Int] = List.range(minExtentYear, year + 1)
        val diffValue: Double = diffYears.foldLeft(0.0)((acc, diffYear) => acc + diff.getOrElse(diffYear - shift, 0.0))
        (year, baseValue - diffValue)
      }): _*))
    )

  }

  def empty: ForestChangeDiagnosticDataValueYearly =
    ForestChangeDiagnosticDataValueYearly(SortedMap())

  def prefilled: ForestChangeDiagnosticDataValueYearly = {
    val minLossYear = ForestChangeDiagnosticCommand.ForestLossYearStart
    val maxLossYear = ForestChangeDiagnosticCommand.ForestLossYearEnd
    val kvList = (for (i <- minLossYear to maxLossYear) yield(i -> 0.0))

    ForestChangeDiagnosticDataValueYearly(
      SortedMap(kvList.toSeq: _*)
    )
  }

  def fromString(value: String): ForestChangeDiagnosticDataValueYearly = {
    val sortedMap = decode[SortedMap[Int, Double]](value)
    ForestChangeDiagnosticDataValueYearly(sortedMap.getOrElse(SortedMap()))
  }

  implicit def injection: Injection[ForestChangeDiagnosticDataValueYearly, String] = Injection(_.toJson, fromString)
}
