package org.globalforestwatch.summarystats.forest_change_diagnostic

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

}
