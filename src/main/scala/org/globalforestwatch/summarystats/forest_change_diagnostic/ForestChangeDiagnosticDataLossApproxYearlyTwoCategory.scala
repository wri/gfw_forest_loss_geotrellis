package org.globalforestwatch.summarystats.forest_change_diagnostic

import frameless.Injection
import io.circe.syntax._
import io.circe.parser.decode

import org.globalforestwatch.layers.ApproxYear

// Two levels of categorization that lead to approximate yearly forest loss objects
// (ForestChangeDiagnosticDataLossApproxYearly).
case class ForestChangeDiagnosticDataLossApproxYearlyTwoCategory(
                                                         value: Map[String, ForestChangeDiagnosticDataLossApproxYearlyCategory]
                                                       ) extends ForestChangeDiagnosticDataParser[
  ForestChangeDiagnosticDataLossApproxYearlyTwoCategory
] {
  def merge(
             other: ForestChangeDiagnosticDataLossApproxYearlyTwoCategory
           ): ForestChangeDiagnosticDataLossApproxYearlyTwoCategory = {

    ForestChangeDiagnosticDataLossApproxYearlyTwoCategory(value ++ other.value.map {
      case (key, otherValue) =>
        key -> value
          .getOrElse(key, ForestChangeDiagnosticDataLossApproxYearlyCategory.empty)
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

object ForestChangeDiagnosticDataLossApproxYearlyTwoCategory {
  def empty: ForestChangeDiagnosticDataLossApproxYearlyTwoCategory =
    ForestChangeDiagnosticDataLossApproxYearlyTwoCategory(Map())

  // There is a breakdown by two levels of categories before the breakdown into
  // approx-yearly forest loss.
  def fill(
            categoryName: String,
            categoryName2: String,
            lossYear: ApproxYear,
            areaHa: Double,
            noData: List[String] = List("", "Unknown", "Not applicable"),
            include: Boolean = true
          ): ForestChangeDiagnosticDataLossApproxYearlyTwoCategory = {

    if (noData.contains(categoryName))
      ForestChangeDiagnosticDataLossApproxYearlyTwoCategory.empty
    else
      ForestChangeDiagnosticDataLossApproxYearlyTwoCategory(
        Map(
          categoryName -> ForestChangeDiagnosticDataLossApproxYearlyCategory
            .fill(categoryName2, lossYear, areaHa, include = include)
        )
      )
  }

  def fromString(
                  value: String
                ): ForestChangeDiagnosticDataLossApproxYearlyTwoCategory = {

    val categories: Map[String, String] =
      decode[Map[String, String]](value).getOrElse(Map())
    val newValues = categories.map {
      case (k, v) => (k, ForestChangeDiagnosticDataLossApproxYearlyCategory.fromString(v))
    }

    ForestChangeDiagnosticDataLossApproxYearlyTwoCategory(newValues)

  }

  // See https://typelevel.org/frameless/Injection.html and
  // https://typelevel.org/frameless/TypedEncoder.html
  // Has an implicit TypedEncoder based on this injection in package.scala
  implicit def injection: Injection[ForestChangeDiagnosticDataLossApproxYearlyTwoCategory, String] = Injection(_.toJson, fromString)
}
