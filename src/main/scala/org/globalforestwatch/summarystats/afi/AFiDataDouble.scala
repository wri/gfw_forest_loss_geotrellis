package org.globalforestwatch.summarystats.afi
import frameless.Injection
import org.globalforestwatch.util.Implicits._
import io.circe.syntax._

case class AFiDataDouble(value: Double) extends AFiDataParser[AFiDataDouble] {
  def merge(
             other: AFiDataDouble
           ): AFiDataDouble = {
    AFiDataDouble(value + other.value)
  }

  def round: Double = this.round(value)

  def toJson: String = {
    this.round.asJson.noSpaces
  }
}

object AFiDataDouble {
  def empty: AFiDataDouble =
    AFiDataDouble(0)

  def fill(value: Double,
           include: Boolean = true): AFiDataDouble = {
    AFiDataDouble(value * include)
  }

  implicit def injection: Injection[AFiDataDouble, String] =
    Injection(_.toJson, s => AFiDataDouble(s.toDouble))

}


