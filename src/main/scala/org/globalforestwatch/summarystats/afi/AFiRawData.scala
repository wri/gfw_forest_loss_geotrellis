package org.globalforestwatch.summarystats.afi

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class AFiRawData(
                       var treeCoverLossArea: Double,
                       var naturalLandExtent: Double,
                       var isNegligibleRisk: Boolean,
                       var negligibleRiskArea: Double,
                       var totalArea: Double
       ) {
  def merge(other: AFiRawData): AFiRawData = {
    AFiRawData(
      treeCoverLossArea + other.treeCoverLossArea,
      naturalLandExtent + other.naturalLandExtent,
      isNegligibleRisk || other.isNegligibleRisk,
      negligibleRiskArea + other.negligibleRiskArea,
      totalArea + other.totalArea,
  }
}

object AFiRawData {
  implicit val lossDataSemigroup: Semigroup[AFiRawData] = Semigroup.instance(_ merge _)
}
