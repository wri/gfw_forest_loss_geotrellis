package org.globalforestwatch.summarystats.forest_change_diagnostic

import org.globalforestwatch.util.Implicits._
import scala.collection.immutable.SortedMap

case class ForestChangeDiagnosticDataLossYearlyCategory(
                                                         value: Map[String, ForestChangeDiagnosticDataLossYearly]
) {
  def merge(
             other: ForestChangeDiagnosticDataLossYearlyCategory
           ): ForestChangeDiagnosticDataLossYearlyCategory = {
    ForestChangeDiagnosticDataLossYearlyCategory(value ++ other.value.map {
      case (key, otherValue) =>
        key -> value(key).merge(otherValue)
    })
  }
}


object ForestChangeDiagnosticDataLossYearlyCategory {
  def empty: ForestChangeDiagnosticDataLossYearlyCategory =
    ForestChangeDiagnosticDataLossYearlyCategory(
      Map()
    )

  def fill(className: String, lossYear: Int, areaHa: Double, noData: String, include: Boolean = true): ForestChangeDiagnosticDataLossYearlyCategory = {

    className match {
      case noData =>
        ForestChangeDiagnosticDataLossYearlyCategory.empty
      case _ =>
        ForestChangeDiagnosticDataLossYearlyCategory(
          Map(
            className -> ForestChangeDiagnosticDataLossYearly(
              SortedMap(lossYear -> areaHa * include)
            )
          )
        )
    }
  }

}
