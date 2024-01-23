package org.globalforestwatch.summarystats.forest_change_diagnostic

import frameless.Injection

import scala.collection.immutable.SortedMap
import io.circe.syntax._
import io.circe.parser.decode
import cats.kernel.Semigroup
import cats.implicits._

case class ForestChangeDiagnosticDataLossApproxYearly(value: SortedMap[String, Double])
  extends ForestChangeDiagnosticDataParser[ForestChangeDiagnosticDataLossApproxYearly] {

  def merge(other: ForestChangeDiagnosticDataLossApproxYearly): ForestChangeDiagnosticDataLossApproxYearly = {
    ForestChangeDiagnosticDataLossApproxYearly(Semigroup[SortedMap[String, Double]].combine(value, other.value))
  }

  def mergeApprox: ForestChangeDiagnosticDataLossApproxYearly = {
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
    this.mergeApprox.round.asJson.noSpaces
  }
}

object ForestChangeDiagnosticDataLossApproxYearly {
  def empty: ForestChangeDiagnosticDataLossApproxYearly =
    ForestChangeDiagnosticDataLossApproxYearly(
      SortedMap()
    )

  def prefilled: ForestChangeDiagnosticDataLossApproxYearly = {
    val kvList = (for (i <- 2001 to 2022) yield(i.toString -> 0.0)) ++
       (for (i <- 2101 to 2122) yield(i.toString -> 0.0))

    ForestChangeDiagnosticDataLossApproxYearly(
      SortedMap(kvList.toSeq: _*)
    )
  }

  def fill(lossYear: Int,
           areaHa: Double,
           include: Boolean = true): ForestChangeDiagnosticDataLossApproxYearly = {

    // Only except lossYear values within range of default map
    val minLossYear: Int = 2001
    val maxLossYear: Int = 2122

    if (minLossYear <= lossYear && lossYear <= maxLossYear && include) {
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
