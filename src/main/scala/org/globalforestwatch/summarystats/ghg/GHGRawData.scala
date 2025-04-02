package org.globalforestwatch.summarystats.ghg


import cats.Semigroup

/** Summary data per RawDataGroup.
  *
  * Note: This case class contains mutable values to do accumulation by group
  * in GHGSummary.getGridVisitor.
  */
case class GHGRawData(
  var totalArea: Double,        // in hectares
  var emissionsCo2eCO2: Double, // in Mg CO2e (metric tonnes CO2e)
  var emissionsCo2eCH4: Double, // in Mg CO2e (metric tonnes CO2e)
  var emissionsCo2eN2O: Double, // in Mg CO2e (metric tonnes CO2e)
  var emissionsCo2e: Double     // in Mg CO2e (metric tonnes CO2e)
) {
  def merge(other: GHGRawData): GHGRawData = {
    GHGRawData(totalArea + other.totalArea,
      emissionsCo2eCO2 + other.emissionsCo2eCO2,
      emissionsCo2eCH4 + other.emissionsCo2eCH4,
      emissionsCo2eN2O + other.emissionsCo2eN2O,
      emissionsCo2e + other.emissionsCo2e)
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
