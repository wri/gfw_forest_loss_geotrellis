package org.globalforestwatch.summarystats.forest_change_diagnostic

import frameless.Injection
import io.circe.syntax._
import io.circe.parser.decode

case class ForestChangeDiagnosticDataDoubleTwoCategory(
  value: Map[String, ForestChangeDiagnosticDataDoubleCategory]
                                                   ) extends ForestChangeDiagnosticDataParser[ForestChangeDiagnosticDataDoubleTwoCategory] {
  def merge(
    other: ForestChangeDiagnosticDataDoubleTwoCategory
  ): ForestChangeDiagnosticDataDoubleTwoCategory = {

    ForestChangeDiagnosticDataDoubleTwoCategory(value ++ other.value.map {
      case (key, otherValue) =>
        key -> value
          .getOrElse(key, ForestChangeDiagnosticDataDoubleCategory.empty)
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

object ForestChangeDiagnosticDataDoubleTwoCategory {
  def empty: ForestChangeDiagnosticDataDoubleTwoCategory =
    ForestChangeDiagnosticDataDoubleTwoCategory(Map())

  def fill(
            className1: String,
            className2: String,
            areaHa: Double,
            noData: List[String] = List("", "Unknown", "Not applicable"),
            include: Boolean = true
          ): ForestChangeDiagnosticDataDoubleTwoCategory = {
    if (noData.contains(className1))
      ForestChangeDiagnosticDataDoubleTwoCategory.empty
    else
      ForestChangeDiagnosticDataDoubleTwoCategory(
        Map(className1 -> ForestChangeDiagnosticDataDoubleCategory.fill(className2, areaHa, include = include))
      )
  }

  def fromString(value: String): ForestChangeDiagnosticDataDoubleTwoCategory = {

    val categories: Map[String, String] = decode[Map[String, String]](value).getOrElse(Map())
    val newValue: Map[String, ForestChangeDiagnosticDataDoubleCategory] = categories.map { case (k, v) => (k, ForestChangeDiagnosticDataDoubleCategory.fromString(v)) }
    ForestChangeDiagnosticDataDoubleTwoCategory(newValue)

  }

  implicit def injection: Injection[ForestChangeDiagnosticDataDoubleTwoCategory , String] = Injection(_.toJson, fromString)
}

