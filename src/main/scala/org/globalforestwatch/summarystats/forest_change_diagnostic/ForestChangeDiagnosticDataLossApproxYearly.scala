package org.globalforestwatch.summarystats.forest_change_diagnostic

import frameless.Injection

import scala.collection.immutable.SortedMap
import io.circe.syntax._
import io.circe.parser.decode
import cats.kernel.Semigroup
import cats.implicits._

// This class is for recording forest loss yearly, but also marking cases where the
// year of loss is approximate vs exact.

case class ForestChangeDiagnosticDataLossApproxYearly(value: SortedMap[String, Double])
  extends ForestChangeDiagnosticDataParser[ForestChangeDiagnosticDataLossApproxYearly] {

  def merge(other: ForestChangeDiagnosticDataLossApproxYearly): ForestChangeDiagnosticDataLossApproxYearly = {
    ForestChangeDiagnosticDataLossApproxYearly(Semigroup[SortedMap[String, Double]].combine(value, other.value))
  }

  // This function combines years with both approximate and exact loss into a single
  // year with the "approx" suffix, while a year with exact loss only is still put
  // under the indicated year without the "approx" suffix.
  def combineApprox: ForestChangeDiagnosticDataLossApproxYearly = {
    var out = scala.collection.mutable.SortedMap[String, Double]()
    for ((k, v) <- this.value) {
      if (k.toInt >= 2101) {
        if (v > 0) {
          val origYearStr = (k.toInt - 100).toString
          out(origYearStr + "approx") = v
          if (out.contains(origYearStr)) {
            out(origYearStr + "approx") += out(origYearStr)
            out -= origYearStr
          }
        }
      } else if (out.contains(k + "approx")) {
          if (v > 0) {
            out(k + "approx") += v
          }
      } else {
        out(k) = v
      }
    }
    ForestChangeDiagnosticDataLossApproxYearly(SortedMap(out.toSeq: _*))
  }

  def round: SortedMap[String, Double] = this.value.map { case (key, value) => key -> this.round(value) }

  def toJson: String = {
    this.combineApprox.round.asJson.noSpaces
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
    val kvList = (for (i <- minLossYear to maxLossYear) yield(i.toString -> 0.0)) ++
       (for (i <- minLossYear+100 to maxLossYear+100) yield(i.toString -> 0.0))

    ForestChangeDiagnosticDataLossApproxYearly(
      SortedMap(kvList.toSeq: _*)
    )
  }

  def fill(lossYear: Int,
           areaHa: Double,
           include: Boolean = true): ForestChangeDiagnosticDataLossApproxYearly = {

    // Only accept lossYear values within range of default map
    val minLossYear = ForestChangeDiagnosticCommand.ForestLossYearStart
    val maxLossYear = ForestChangeDiagnosticCommand.ForestLossYearEnd

    if ((minLossYear <= lossYear && lossYear <= maxLossYear ||
         minLossYear + 100 <= lossYear && lossYear <= maxLossYear + 100) && include) {
      ForestChangeDiagnosticDataLossApproxYearly.prefilled.merge(
        ForestChangeDiagnosticDataLossApproxYearly(
          SortedMap(
            lossYear.toString -> areaHa
          )
        )
      )
    } else
      this.empty
  }

  def fromString(value: String): ForestChangeDiagnosticDataLossApproxYearly = {
    val sortedMap = decode[SortedMap[String, Double]](value)
    ForestChangeDiagnosticDataLossApproxYearly(sortedMap.getOrElse(SortedMap()))
  }

  implicit def injection: Injection[ForestChangeDiagnosticDataLossApproxYearly, String] = Injection(_.toJson, fromString)
}
