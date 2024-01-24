package org.globalforestwatch.summarystats.forest_change_diagnostic

import frameless.Injection

import scala.collection.immutable.SortedMap
import io.circe.syntax._
import io.circe.parser.decode
import cats.kernel.Semigroup
import cats.implicits._

case class ForestChangeDiagnosticDataLossYearly(value: SortedMap[Int, Double])
  extends ForestChangeDiagnosticDataParser[ForestChangeDiagnosticDataLossYearly] {

  def merge(other: ForestChangeDiagnosticDataLossYearly): ForestChangeDiagnosticDataLossYearly = {
    ForestChangeDiagnosticDataLossYearly(Semigroup[SortedMap[Int, Double]].combine(value, other.value))
  }

  def round: SortedMap[Int, Double] = this.value.map { case (key, value) => key -> this.round(value) }

  def limitToMaxYear(maxYear: Int): ForestChangeDiagnosticDataLossYearly = {
    ForestChangeDiagnosticDataLossYearly(value.filterKeys{ year => year <= maxYear })
  }

  def toJson: String = {
    this.round.asJson.noSpaces
  }
}

object ForestChangeDiagnosticDataLossYearly {
  def empty: ForestChangeDiagnosticDataLossYearly =
    ForestChangeDiagnosticDataLossYearly(
      SortedMap()
    )

  def prefilled: ForestChangeDiagnosticDataLossYearly = {
    val minLossYear = ForestChangeDiagnosticCommand.ForestLossYearStart
    val maxLossYear = ForestChangeDiagnosticCommand.ForestLossYearEnd
    val kvList = for (i <- minLossYear to maxLossYear) yield(i -> 0.0)
    ForestChangeDiagnosticDataLossYearly(
      SortedMap(kvList.toSeq: _*)
    )
  }

  def fill(lossYear: Int,
           areaHa: Double,
           include: Boolean = true): ForestChangeDiagnosticDataLossYearly = {

    // Only accept lossYear values within range of default map
    val minLossYear = ForestChangeDiagnosticCommand.ForestLossYearStart
    val maxLossYear = ForestChangeDiagnosticCommand.ForestLossYearEnd

    if (minLossYear <= lossYear && lossYear <= maxLossYear && include) {
      ForestChangeDiagnosticDataLossYearly.prefilled.merge(
        ForestChangeDiagnosticDataLossYearly(
          SortedMap(
            lossYear -> areaHa
          )
        )
      )
    } else
      this.empty
  }

  def fromString(value: String): ForestChangeDiagnosticDataLossYearly = {
    val sortedMap = decode[SortedMap[Int, Double]](value)
    ForestChangeDiagnosticDataLossYearly(sortedMap.getOrElse(SortedMap()))
  }

  implicit def injection: Injection[ForestChangeDiagnosticDataLossYearly, String] = Injection(_.toJson, fromString)
}
