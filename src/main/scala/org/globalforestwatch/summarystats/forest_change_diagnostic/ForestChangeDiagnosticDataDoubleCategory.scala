package org.globalforestwatch.summarystats.forest_change_diagnostic
import org.globalforestwatch.util.Implicits._

case class ForestChangeDiagnosticDataDoubleCategory(
  value: Map[String, ForestChangeDiagnosticDataDouble]
) {
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
}

object ForestChangeDiagnosticDataDoubleCategory {
  def empty: ForestChangeDiagnosticDataDoubleCategory =
    ForestChangeDiagnosticDataDoubleCategory(Map())

  def fill(
    className: String,
    areaHa: Double,
    noData: String,
    include: Boolean = true
  ): ForestChangeDiagnosticDataDoubleCategory = {

    className match {
      case `noData` =>
        ForestChangeDiagnosticDataDoubleCategory.empty
      case _ =>
        ForestChangeDiagnosticDataDoubleCategory(
          Map(className -> ForestChangeDiagnosticDataDouble(areaHa * include))
        )
    }
  }

}
