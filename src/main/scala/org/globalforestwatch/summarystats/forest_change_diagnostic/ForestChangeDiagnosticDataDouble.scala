package org.globalforestwatch.summarystats.forest_change_diagnostic
import org.globalforestwatch.util.Implicits._

case class ForestChangeDiagnosticDataDouble(value: Double) extends ValueParser {
  def merge(
    other: ForestChangeDiagnosticDataDouble
  ): ForestChangeDiagnosticDataDouble = {
    ForestChangeDiagnosticDataDouble(value + other.value)
  }
}

object ForestChangeDiagnosticDataDouble {
  def empty: ForestChangeDiagnosticDataDouble =
    ForestChangeDiagnosticDataDouble(0)

  def fill(value: Double,
           include: Boolean = true): ForestChangeDiagnosticDataDouble = {
    ForestChangeDiagnosticDataDouble(value * include)
  }

}
