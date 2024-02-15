package org.globalforestwatch.summarystats.forest_change_diagnostic

import frameless.Injection
import io.circe.syntax._
import io.circe.parser.decode

import org.globalforestwatch.layers.ApproxYear

case class ForestChangeDiagnosticDataLossApproxYearlyCategory(
                                                         value: Map[String, ForestChangeDiagnosticDataLossApproxYearly]
                                                       ) extends ForestChangeDiagnosticDataParser[
  ForestChangeDiagnosticDataLossApproxYearlyCategory
] {
  def merge(
             other: ForestChangeDiagnosticDataLossApproxYearlyCategory
           ): ForestChangeDiagnosticDataLossApproxYearlyCategory = {

    ForestChangeDiagnosticDataLossApproxYearlyCategory(value ++ other.value.map {
      case (key, otherValue) =>
        key -> value
          .getOrElse(key, ForestChangeDiagnosticDataLossApproxYearly.empty)
          .merge(otherValue)
    })
  }

  def toJson: String = {
    val x = this.value
      .map {
        case (key, value) =>
          key -> value.formatAndRound
      }

    x.asJson
    .noSpaces
  }
}

object ForestChangeDiagnosticDataLossApproxYearlyCategory {
  def empty: ForestChangeDiagnosticDataLossApproxYearlyCategory =
    ForestChangeDiagnosticDataLossApproxYearlyCategory(Map())

  def fill(
            className: String,
            lossYear: ApproxYear,
            areaHa: Double,
            noData: List[String] = List("", "Unknown", "Not applicable"),
            include: Boolean = true
          ): ForestChangeDiagnosticDataLossApproxYearlyCategory = {

    if (noData.contains(className))
      ForestChangeDiagnosticDataLossApproxYearlyCategory.empty
    else
      ForestChangeDiagnosticDataLossApproxYearlyCategory(
        Map(
          className -> ForestChangeDiagnosticDataLossApproxYearly
            .fill(lossYear, areaHa, include)
        )
      )
  }

  def fromString(
                  value: String
                ): ForestChangeDiagnosticDataLossApproxYearlyCategory = {

    val categories: Map[String, String] =
      decode[Map[String, String]](value).getOrElse(Map())
    val newValues = categories.map {
      case (k, v) => (k, ForestChangeDiagnosticDataLossApproxYearly.fromString(v))
    }

    ForestChangeDiagnosticDataLossApproxYearlyCategory(newValues)

  }

  // See https://typelevel.org/frameless/Injection.html and
  // https://typelevel.org/frameless/TypedEncoder.html
  // Has an implicit TypedEncoder based on this injection in package.scala
  implicit def injection: Injection[ForestChangeDiagnosticDataLossApproxYearlyCategory, String] = Injection(_.toJson, fromString)
  //implicit def convertToString(f: ForestChangeDiagnosticDataLossApproxYearlyCategory): String =  f.toJson

}
