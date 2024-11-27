package org.globalforestwatch.summarystats.ghg
import frameless.Injection
import org.globalforestwatch.util.Implicits._
import io.circe.syntax._


case class GHGDataDouble(value: Double) extends GHGDataParser[GHGDataDouble] {
  def merge(
             other: GHGDataDouble
           ): GHGDataDouble = {
    GHGDataDouble(value + other.value)
  }

  def round: Double = this.round(value)

  def toJson: String = {
    this.round.asJson.noSpaces
  }
}

object GHGDataDouble {
  def empty: GHGDataDouble =
    GHGDataDouble(0)

  def fill(value: Double,
           include: Boolean = true): GHGDataDouble = {
    GHGDataDouble(value * include)
  }

  implicit def injection: Injection[GHGDataDouble, String] =
    Injection(_.toJson, s => GHGDataDouble(s.toDouble))

}
