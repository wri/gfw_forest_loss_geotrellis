package org.globalforestwatch.summarystats.afi

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class AFiRawData(var treeCoverLossArea: Double) {
  def merge(other: AFiRawData): AFiRawData = {
    AFiRawData(treeCoverLossArea + other.treeCoverLossArea)
  }
}

object AFiRawData {
  implicit val lossDataSemigroup: Semigroup[AFiRawData] = Semigroup.instance(_ merge _)
}
