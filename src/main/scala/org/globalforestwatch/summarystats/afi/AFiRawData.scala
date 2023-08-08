package org.globalforestwatch.summarystats.afi

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class AFiRawData(var totalArea: Double) {
  def merge(other: AFiRawData): AFiRawData = {
    AFiRawData(
      totalArea + other.totalArea
  }
}

object AFiRawData {
  implicit val lossDataSemigroup: Semigroup[AFiRawData] = Semigroup.instance(_ merge _)
}
