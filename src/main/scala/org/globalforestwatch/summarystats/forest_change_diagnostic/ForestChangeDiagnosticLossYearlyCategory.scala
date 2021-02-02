package org.globalforestwatch.summarystats.forest_change_diagnostic

import org.globalforestwatch.util.Implicits._
import scala.collection.immutable.SortedMap

case class ForestChangeDiagnosticLossYearlyCategory(
                                                     stats: Map[String, ForestChangeDiagnosticLossYearly]
) {
  def merge(
             other: ForestChangeDiagnosticLossYearlyCategory
           ): ForestChangeDiagnosticLossYearlyCategory = {
    ForestChangeDiagnosticLossYearlyCategory(stats ++ other.stats.map {
      case (key, otherValue) =>
        key -> stats(key).merge(otherValue)
    })
  }
}


object ForestChangeDiagnosticLossYearlyCategory {
  def empty: ForestChangeDiagnosticLossYearlyCategory =
    ForestChangeDiagnosticLossYearlyCategory(
      Map()
    )

  def fill(className: String, lossYear: Int, areaHa: Double, noData: String, include: Boolean = true): ForestChangeDiagnosticLossYearlyCategory = {

    className match {
      case noData =>
        ForestChangeDiagnosticLossYearlyCategory.empty
      case _ =>
        ForestChangeDiagnosticLossYearlyCategory(
          Map(
            className -> ForestChangeDiagnosticLossYearly(
              SortedMap(lossYear -> areaHa * include)
            )
          )
        )
    }
  }

}
