package org.globalforestwatch.summarystats.forest_change_diagnostic

import frameless.Injection
import io.circe.syntax._
import io.circe.parser.decode


// Two levels of categorization that lead to yearly forest loss objects
// (ForestChangeDiagnosticDataLossYearly).
case class ForestChangeDiagnosticDataLossYearlyTwoCategory(
                                                         value: Map[String, ForestChangeDiagnosticDataLossYearlyCategory]
                                                       ) extends ForestChangeDiagnosticDataParser[
  ForestChangeDiagnosticDataLossYearlyTwoCategory
] {
  def merge(
             other: ForestChangeDiagnosticDataLossYearlyTwoCategory
           ): ForestChangeDiagnosticDataLossYearlyTwoCategory = {

    ForestChangeDiagnosticDataLossYearlyTwoCategory(value ++ other.value.map {
      case (key, otherValue) =>
        key -> value
          .getOrElse(key, ForestChangeDiagnosticDataLossYearlyCategory.empty)
          .merge(otherValue)
    })
  }

  def toJson: String = {
    this.value
      .map {
        case (key, value) =>
          key -> value.round
      }
      .asJson
      .noSpaces
  }
}

object ForestChangeDiagnosticDataLossYearlyTwoCategory {
  def empty: ForestChangeDiagnosticDataLossYearlyTwoCategory =
    ForestChangeDiagnosticDataLossYearlyTwoCategory(Map())

  // There is a breakdown by two levels of categories before the breakdown into
  // yearly forest loss.
  def fill(
            categoryName: String,
            categoryName2: String,
            lossYear: Int,
            areaHa: Double,
            noData: List[String] = List("", "Unknown", "Not applicable"),
            include: Boolean = true
          ): ForestChangeDiagnosticDataLossYearlyTwoCategory = {

    if (noData.contains(categoryName) || !include)
      ForestChangeDiagnosticDataLossYearlyTwoCategory.empty
    else
      ForestChangeDiagnosticDataLossYearlyTwoCategory(
        Map(
          categoryName -> ForestChangeDiagnosticDataLossYearlyCategory
            .fill(categoryName2, lossYear, areaHa, include = include)
        )
      )
  }

  def fromString(
                  value: String
                ): ForestChangeDiagnosticDataLossYearlyTwoCategory = {

    val categories: Map[String, String] =
      decode[Map[String, String]](value).getOrElse(Map())
    val newValues = categories.map {
      case (k, v) => (k, ForestChangeDiagnosticDataLossYearlyCategory.fromString(v))
    }

    ForestChangeDiagnosticDataLossYearlyTwoCategory(newValues)

  }

  // See https://typelevel.org/frameless/Injection.html and
  // https://typelevel.org/frameless/TypedEncoder.html
  // Has an implicit TypedEncoder based on this injection in package.scala
  implicit def injection: Injection[ForestChangeDiagnosticDataLossYearlyTwoCategory, String] = Injection(_.toJson, fromString)
}
