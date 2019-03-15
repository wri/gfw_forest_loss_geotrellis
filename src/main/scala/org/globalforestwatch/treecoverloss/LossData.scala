package org.globalforestwatch.treecoverloss

import cats.{Monoid, Semigroup}
import geotrellis.raster.histogram.StreamingHistogram

/** Summary data per class
  * Note: This case class contains mutable values
  *
  * @param totalArea sum of pixel area
  * @param totalCo2 sum of co2 pixel values
  * @param totalGainArea sum of gain pixel area
  */
case class LossData(var totalArea: Double, var totalCo2: Double, var totalGainArea: Double) {
  def merge(other: LossData): LossData = {
    LossData(totalArea + other.totalArea, totalCo2 + other.totalCo2, totalGainArea + other.totalGainArea)
  }
}

object LossData {
  implicit val lossDataSemigroup = new Semigroup[LossData] {
    def combine(x: LossData, y: LossData): LossData = x.merge(y)
  }
}