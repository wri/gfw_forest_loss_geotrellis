package org.globalforestwatch.summarystats.afi

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class AFiRawData(var lossArea: Double) {
  def merge(other: AFiRawData): AFiRawData = {
    AFiRawData(lossArea + other.lossArea)
  }
}

object AFiRawData {
  implicit val lossDataSemigroup: Semigroup[AFiRawData] = Semigroup.instance(_ merge _)
}
