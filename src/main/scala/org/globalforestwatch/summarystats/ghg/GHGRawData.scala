package org.globalforestwatch.summarystats.ghg


import cats.Semigroup

/** Summary data per RawDataGroup.
  *
  * Note: This case class contains mutable values to do accumulation by group
  * in GHGSummary.getGridVisitor.
  */
case class GHGRawData(var totalArea: Double, var emissionsCo2e: Double) {
  def merge(other: GHGRawData): GHGRawData = {
    GHGRawData(totalArea + other.totalArea, emissionsCo2e + other.emissionsCo2e)
  }
}

object GHGRawData {
  implicit val lossDataSemigroup: Semigroup[GHGRawData] =
    new Semigroup[GHGRawData] {
      def combine(x: GHGRawData,
                  y: GHGRawData): GHGRawData =
        x.merge(y)
    }

}