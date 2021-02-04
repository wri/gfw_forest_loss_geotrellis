package org.globalforestwatch.summarystats.forest_change_diagnostic

import io.circe.syntax._

case class ForestChangeDiagnosticDataBoolean(value: Boolean)
    extends ValueParser[ForestChangeDiagnosticDataBoolean] {
  def merge(
    other: ForestChangeDiagnosticDataBoolean
  ): ForestChangeDiagnosticDataBoolean = {
    ForestChangeDiagnosticDataBoolean((value || other.value))
  }

  def toJson: String = {
    this.value.asJson.noSpaces
  }
}

object ForestChangeDiagnosticDataBoolean {
  def empty: ForestChangeDiagnosticDataBoolean =
    ForestChangeDiagnosticDataBoolean(false)

  def fill(value: Boolean): ForestChangeDiagnosticDataBoolean = {
    ForestChangeDiagnosticDataBoolean(value)
  }

}
