package org.globalforestwatch.summarystats.forest_change_diagnostic

import frameless.Injection

import scala.collection.immutable.SortedMap
import io.circe.syntax._
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
    for ((k, v) <- this.value) {
      val yearString = k.year.toString
      if (k.approx) {
        if (v > 0) {
          out(yearString + "approx") = v
          if (out.contains(yearString)) {
            out(yearString + "approx") += out(yearString)
            out -= yearString
          }
        }
      } else if (out.contains(yearString + "approx")) {
          if (v > 0) {
            out(yearString + "approx") += v
          }
      } else {
        out(yearString) = v
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
    val minLossYear = ForestChangeDiagnosticCommand.ForestLossYearStart
    val maxLossYear = ForestChangeDiagnosticCommand.ForestLossYearEnd
    val kvList = (for (i <- minLossYear to maxLossYear) yield(ApproxYear(i, false) -> 0.0))

    ForestChangeDiagnosticDataLossApproxYearly(
      SortedMap(kvList.toSeq: _*)
    )
  }

  def fill(lossYear: ApproxYear,
           areaHa: Double,
           include: Boolean = true): ForestChangeDiagnosticDataLossApproxYearly = {

    // Only accept lossYear values within range of default map
    val minLossYear = ForestChangeDiagnosticCommand.ForestLossYearStart
    val maxLossYear = ForestChangeDiagnosticCommand.ForestLossYearEnd

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
    // I'm not deriving the decoder for SortedMap[ApproxYear, Double], since
    // we don't every decode any ForestChangeDiagnosticDataLossApproxYearly.
    ForestChangeDiagnosticDataLossApproxYearly(SortedMap())
  }

  implicit def injection: Injection[ForestChangeDiagnosticDataLossApproxYearly, String] = Injection(_.toJson, fromString)
}
