package org.globalforestwatch.summarystats.afi

import cats.Semigroup
import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticDataLossYearly

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class AFiData(
                    var natural_forest__extent: Double,
                    var natural_forest_loss__ha: Double,
                    var natural_forest_loss_by_year__ha: ForestChangeDiagnosticDataLossYearly,
                    var jrc_forest_cover__extent: Double,
                    var jrc_forest_cover_loss__ha: Double,
                    var jrc_forest_loss_by_year__ha: ForestChangeDiagnosticDataLossYearly,
                    var negligible_risk_area__ha: Double,
                    var total_area__ha: Double
                  ) {
  def merge(other: AFiData): AFiData = {
    AFiData(
      natural_forest__extent + other.natural_forest__extent,
      natural_forest_loss__ha + other.natural_forest_loss__ha,
      natural_forest_loss_by_year__ha.merge(other.natural_forest_loss_by_year__ha),
      jrc_forest_cover__extent + other.jrc_forest_cover__extent,
      jrc_forest_cover_loss__ha + other.jrc_forest_cover_loss__ha,
      jrc_forest_loss_by_year__ha.merge(other.jrc_forest_loss_by_year__ha),
      negligible_risk_area__ha + other.negligible_risk_area__ha,
      total_area__ha + other.total_area__ha
    )
  }
}

object AFiData {
  def empty: AFiData =
    AFiData(0, 0, ForestChangeDiagnosticDataLossYearly.empty,
      0, 0, ForestChangeDiagnosticDataLossYearly.empty, 0, 0)

  implicit val afiDataSemigroup: Semigroup[AFiData] =
    new Semigroup[AFiData] {
      def combine(x: AFiData, y: AFiData): AFiData = x.merge(y)
    }

}
