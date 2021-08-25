package org.globalforestwatch.summarystats.forest_change_diagnostic
import frameless.{Injection, TypedEncoder}
import org.globalforestwatch.util.Implicits._
import io.circe.syntax._

case class ForestChangeDiagnosticDataDouble(value: Double) extends ForestChangeDiagnosticDataParser[ForestChangeDiagnosticDataDouble] {
  def merge(
             other: ForestChangeDiagnosticDataDouble
           ): ForestChangeDiagnosticDataDouble = {
    ForestChangeDiagnosticDataDouble(value + other.value)
  }

  def round: Double = this.round(value)

  def toJson: String = {
    this.round.asJson.noSpaces
  }
}

object ForestChangeDiagnosticDataDouble {
  def empty: ForestChangeDiagnosticDataDouble =
    ForestChangeDiagnosticDataDouble(0)

  def fill(value: Double,
           include: Boolean = true): ForestChangeDiagnosticDataDouble = {
    ForestChangeDiagnosticDataDouble(value * include)
  }

  implicit def injection: Injection[ForestChangeDiagnosticDataDouble, String] =
    Injection(_.toJson, s => ForestChangeDiagnosticDataDouble(s.toDouble))

}
