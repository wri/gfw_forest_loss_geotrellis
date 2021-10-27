package org.globalforestwatch.summarystats.forest_change_diagnostic

import frameless.Injection
import io.circe.syntax._
import org.globalforestwatch.util.Implicits._
import io.circe.parser.decode

case class ForestChangeDiagnosticDataDoubleCategory(
  value: Map[String, ForestChangeDiagnosticDataDouble]
                                                   ) extends ForestChangeDiagnosticDataParser[ForestChangeDiagnosticDataDoubleCategory] {
  def merge(
    other: ForestChangeDiagnosticDataDoubleCategory
  ): ForestChangeDiagnosticDataDoubleCategory = {

    ForestChangeDiagnosticDataDoubleCategory(value ++ other.value.map {
      case (key, otherValue) =>
        key -> value
          .getOrElse(key, ForestChangeDiagnosticDataDouble.empty)
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

object ForestChangeDiagnosticDataDoubleCategory {
  def empty: ForestChangeDiagnosticDataDoubleCategory =
    ForestChangeDiagnosticDataDoubleCategory(Map())

  def fill(
            className: String,
            areaHa: Double,
            noData: List[String] = List("", "Unknown", "Not applicable"),
            include: Boolean = true
          ): ForestChangeDiagnosticDataDoubleCategory = {
    if (noData.contains(className))
      ForestChangeDiagnosticDataDoubleCategory.empty
    else
      ForestChangeDiagnosticDataDoubleCategory(
        Map(className -> ForestChangeDiagnosticDataDouble.fill(areaHa, include))
      )
  }

  def fromString(value: String): ForestChangeDiagnosticDataDoubleCategory = {

    val categories: Map[String, String] = decode[Map[String, String]](value).getOrElse(Map())
    val newValue: Map[String, ForestChangeDiagnosticDataDouble] = categories.map { case (k, v) => (k, ForestChangeDiagnosticDataDouble(v.toDouble)) }
    ForestChangeDiagnosticDataDoubleCategory(newValue)

  }

  implicit def injection: Injection[ForestChangeDiagnosticDataDoubleCategory , String] = Injection(_.toJson, fromString)
}
