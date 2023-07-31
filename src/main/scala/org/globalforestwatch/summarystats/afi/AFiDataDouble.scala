package org.globalforestwatch.summarystats.afi

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


