package org.globalforestwatch.summarystats.forest_change_diagnostic

import frameless.Injection

import scala.collection.immutable.SortedMap
import io.circe.syntax._
import io.circe.parser.decode
import cats.kernel.Semigroup
import cats.implicits._

import org.globalforestwatch.layers.ApproxYear

// This class is for recording forest loss yearly, but also marking cases where the
// year of loss is approximate vs exact.

case class ForestChangeDiagnosticDataLossApproxYearly(value: SortedMap[ApproxYear, Double])
  extends ForestChangeDiagnosticDataParser[ForestChangeDiagnosticDataLossApproxYearly] {

  def merge(other: ForestChangeDiagnosticDataLossApproxYearly): ForestChangeDiagnosticDataLossApproxYearly = {
    ForestChangeDiagnosticDataLossApproxYearly(Semigroup[SortedMap[ApproxYear, Double]].combine(value, other.value))
  }

  // This function combines years with both approximate and exact loss into a single
  // year with the "approx" suffix, while a year with exact loss only is still put
  // under the indicated year without the "approx" suffix.
  def combineApprox: SortedMap[String, Double] = {
    var out = scala.collection.mutable.SortedMap[String, Double]()
    for ((approxYear, lossArea) <- this.value) {
      val yearString = approxYear.year.toString
      if (approxYear.approx) {
        if (lossArea > 0) {
          out(yearString + "approx") = lossArea
          if (out.contains(yearString)) {
            out(yearString + "approx") += out(yearString)
            out -= yearString
          }
        }
      } else if (out.contains(yearString + "approx")) {
          if (lossArea > 0) {
            out(yearString + "approx") += lossArea
          }
      } else {
        out(yearString) = lossArea
      }
    }
    SortedMap(out.toSeq: _*)
  }

  def round(m: SortedMap[String, Double]): SortedMap[String, Double] = m.map { case (key, value) => key -> this.round(value) }

  def toJson: String = {
    this.round(this.combineApprox).asJson.noSpaces
  }
}

object ForestChangeDiagnosticDataLossApproxYearly {
  def empty: ForestChangeDiagnosticDataLossApproxYearly =
    ForestChangeDiagnosticDataLossApproxYearly(
      SortedMap()
    )

  def prefilled: ForestChangeDiagnosticDataLossApproxYearly = {
    val minLossYear = ForestChangeDiagnosticCommand.TreeCoverLossYearStart
    val maxLossYear = ForestChangeDiagnosticCommand.TreeCoverLossYearEnd
    val kvList = for (i <- minLossYear to maxLossYear) yield(ApproxYear(i, false) -> 0.0)

    ForestChangeDiagnosticDataLossApproxYearly(
      SortedMap(kvList.toSeq: _*)
    )
  }

  def fill(lossYear: ApproxYear,
           areaHa: Double,
           include: Boolean = true): ForestChangeDiagnosticDataLossApproxYearly = {

    // Only accept lossYear values within range of default map
    val minLossYear = ForestChangeDiagnosticCommand.TreeCoverLossYearStart
    val maxLossYear = ForestChangeDiagnosticCommand.TreeCoverLossYearEnd

    if (minLossYear <= lossYear.year && lossYear.year <= maxLossYear && include) {
      ForestChangeDiagnosticDataLossApproxYearly.prefilled.merge(
        ForestChangeDiagnosticDataLossApproxYearly(
          SortedMap(
            lossYear -> areaHa
          )
        )
      )
    } else
      this.empty
  }

  def fromString(value: String): ForestChangeDiagnosticDataLossApproxYearly = {
    val sortedMap = decode[SortedMap[ApproxYear, Double]](value)
    ForestChangeDiagnosticDataLossApproxYearly(sortedMap.getOrElse(SortedMap()))
  }

  implicit def injection: Injection[ForestChangeDiagnosticDataLossApproxYearly, String] = Injection(_.toJson, fromString)
}
