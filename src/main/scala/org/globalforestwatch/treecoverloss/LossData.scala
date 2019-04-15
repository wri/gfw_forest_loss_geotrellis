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
case class LossData(var lossYear: scala.collection.mutable.Map[Int, (Double, Double, Double, Double, Double)], var totalArea: Double, var totalGainArea: Double, var totalBiomass: Double,
                    var totalCo2: Double, var biomassHistogram: StreamingHistogram, var totalMangroveBiomass: Double,
                    var totalMangroveCo2: Double, var mangroveBiomassHistogram: StreamingHistogram) {
  def merge(other: LossData): LossData = {

    LossData(
      lossYear ++ other.lossYear.map { case (k, v) => {
        val otherLoss: (Double, Double, Double, Double, Double) = lossYear.getOrElse(k, (0, 0, 0, 0, 0))
        val loss: (Double, Double, Double, Double, Double) = v
        k -> (loss._1 + otherLoss._1,
          loss._2 + otherLoss._2,
          loss._3 + otherLoss._3,
          loss._4 + otherLoss._4,
          loss._5 + otherLoss._5)
      }
      },
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


