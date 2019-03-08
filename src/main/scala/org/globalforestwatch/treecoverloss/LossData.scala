package org.globalforestwatch.treecoverloss

import cats.{Monoid, Semigroup}
import geotrellis.raster.histogram.StreamingHistogram

/** Summary data per class
  * Note: This case class contains mutable values
  *
  * @param tcdHistogram distribution of tree cover density pixels values
  * @param totalCo2 sum of co2 pixel values
  */
case class LossData(tcdHistogram: StreamingHistogram, var totalCo2: Double) {
  def merge(other: LossData): LossData = {
    LossData(tcdHistogram.merge(other.tcdHistogram), totalCo2 + other.totalCo2)
  }
}

object LossData {
  implicit val lossDataSemigroup = new Semigroup[LossData] {
    def combine(x: LossData, y: LossData): LossData = x.merge(y)
  }
}