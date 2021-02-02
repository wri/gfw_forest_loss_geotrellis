package org.globalforestwatch.summarystats.forest_change_diagnostic

import scala.collection.immutable.SortedMap

case class ForestChangeDiagnosticTCLClassYearly(
  stats: Map[String, ForestChangeDiagnosticTCLYearly]
) {
  def merge(
    other: ForestChangeDiagnosticTCLClassYearly
  ): ForestChangeDiagnosticTCLClassYearly = {
    ForestChangeDiagnosticTCLClassYearly(stats ++ other.stats.map {
      case (key, otherValue) =>
        key -> stats(key).merge(otherValue)
    })
  }
}


object ForestChangeDiagnosticTCLClassYearly {
  def empty: ForestChangeDiagnosticTCLClassYearly =
    ForestChangeDiagnosticTCLClassYearly(
      Map()
    )

  def fill(className: String, lossYear: Int, areaHa: Double, noData: String): ForestChangeDiagnosticTCLClassYearly = {

    className match {
      case noData =>
        ForestChangeDiagnosticTCLClassYearly.empty
      case _ =>
        ForestChangeDiagnosticTCLClassYearly(
          Map(
            className -> ForestChangeDiagnosticTCLYearly(
              SortedMap(lossYear -> areaHa)
            )
          )
        )
    }
  }

}
