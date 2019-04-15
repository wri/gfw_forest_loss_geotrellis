package org.globalforestwatch.treecoverloss

import cats.Semigroup
import geotrellis.raster.histogram.StreamingHistogram


/** Summary data per class
  *
  * Note: This case class contains mutable values
  *
  * @param totalArea
  * @param totalGainArea
  * @param totalBiomass
  * @param totalCo2
  * @param biomassHistogram
  * @param totalMangroveBiomass
  * @param totalMangroveCo2
  * @param mangroveBiomassHistogram
  */
case class LossData(var totalArea: Double, var totalGainArea: Double, var totalBiomass: Double,
                    var totalCo2: Double, var biomassHistogram: StreamingHistogram, var totalMangroveBiomass: Double,
                    var totalMangroveCo2: Double, var mangroveBiomassHistogram: StreamingHistogram) {
  def merge(other: LossData): LossData = {

    LossData(
      totalArea + other.totalArea,
      totalGainArea + other.totalGainArea,
      totalBiomass + other.totalBiomass,
      totalCo2 + other.totalCo2,
      biomassHistogram.merge(other.biomassHistogram),
      totalMangroveBiomass + other.totalMangroveBiomass,
      totalMangroveCo2 + other.totalMangroveBiomass,
      mangroveBiomassHistogram.merge(other.mangroveBiomassHistogram)
    )
  }
}

object LossData {
  implicit val lossDataSemigroup: Semigroup[LossData] = new Semigroup[LossData] {
    def combine(x: LossData, y: LossData): LossData = x.merge(y)
  }

}


