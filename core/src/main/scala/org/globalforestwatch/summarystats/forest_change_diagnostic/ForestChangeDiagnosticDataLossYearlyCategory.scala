package org.globalforestwatch.summarystats.forest_change_diagnostic

import frameless.Injection
import io.circe.syntax._
import io.circe.parser.decode


case class ForestChangeDiagnosticDataLossYearlyCategory(
                                                         value: Map[String, ForestChangeDiagnosticDataLossYearly]
                                                       ) extends ForestChangeDiagnosticDataParser[
  ForestChangeDiagnosticDataLossYearlyCategory
] {
  def merge(
             other: ForestChangeDiagnosticDataLossYearlyCategory
           ): ForestChangeDiagnosticDataLossYearlyCategory = {

    ForestChangeDiagnosticDataLossYearlyCategory(value ++ other.value.map {
      case (key, otherValue) =>
        key -> value
          .getOrElse(key, ForestChangeDiagnosticDataLossYearly.empty)
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

object ForestChangeDiagnosticDataLossYearlyCategory {
  def empty: ForestChangeDiagnosticDataLossYearlyCategory =
    ForestChangeDiagnosticDataLossYearlyCategory(Map())

  def fill(
            className: String,
            lossYear: Int,
            areaHa: Double,
            noData: List[String] = List("", "Unknown", "Not applicable"),
            include: Boolean = true
          ): ForestChangeDiagnosticDataLossYearlyCategory = {

    if (noData.contains(className))
      ForestChangeDiagnosticDataLossYearlyCategory.empty
    else
      ForestChangeDiagnosticDataLossYearlyCategory(
        Map(
          className -> ForestChangeDiagnosticDataLossYearly
            .fill(lossYear, areaHa, include)
        )
      )
  }

  def fromString(
                  value: String
                ): ForestChangeDiagnosticDataLossYearlyCategory = {

    val categories: Map[String, String] =
      decode[Map[String, String]](value).getOrElse(Map())
    val newValues = categories.map {
      case (k, v) => (k, ForestChangeDiagnosticDataLossYearly.fromString(v))
    }

    ForestChangeDiagnosticDataLossYearlyCategory(newValues)

  }

  implicit def injection: Injection[ForestChangeDiagnosticDataLossYearlyCategory, String] = Injection(_.toJson, fromString)
}
