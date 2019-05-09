package org.globalforestwatch.gladalerts

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  *
  * @param totalArea
  * @param totalBiomass
  * @param totalCo2
  *
  */
case class GladAlertsData(var totalArea: Double,
                          var totalBiomass: Double,
                          var totalCo2: Double) {
  def merge(other: GladAlertsData): GladAlertsData = {

    GladAlertsData(
      totalArea + other.totalArea,
      totalBiomass + other.totalBiomass,
      totalCo2 + other.totalCo2
    )
  }
}

object GladAlertsData {
  implicit val lossDataSemigroup: Semigroup[GladAlertsData] =
    new Semigroup[GladAlertsData] {
      def combine(x: GladAlertsData, y: GladAlertsData): GladAlertsData =
        x.merge(y)
    }

}
