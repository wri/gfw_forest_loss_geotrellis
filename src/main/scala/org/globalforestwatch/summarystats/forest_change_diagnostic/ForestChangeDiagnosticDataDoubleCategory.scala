package org.globalforestwatch.summarystats.forest_change_diagnostic

import io.circe.syntax._
import org.globalforestwatch.util.Implicits._

case class ForestChangeDiagnosticDataDoubleCategory(
  value: Map[String, ForestChangeDiagnosticDataDouble]
                                                   ) extends ValueParser[ForestChangeDiagnosticDataDoubleCategory] {
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
          key -> value.toJson
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
        Map(className -> ForestChangeDiagnosticDataDouble(areaHa * include))
      )
  }

}
