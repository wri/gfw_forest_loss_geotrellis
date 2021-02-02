package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class ForestChangeDiagnosticData(
                                       treeCoverLossYearly: ForestChangeDiagnosticDataLossYearly,
                                       treeCoverLossPrimaryForestYearly: ForestChangeDiagnosticDataLossYearly,
                                       treeCoverLossPeatLandYearly: ForestChangeDiagnosticDataLossYearly,
                                       treeCoverLossIntactForestYearly: ForestChangeDiagnosticDataLossYearly,
                                       treeCoverLossProtectedAreasYearly: ForestChangeDiagnosticDataLossYearly,
                                       treeCoverLossSEAsiaLandCoverYearly: ForestChangeDiagnosticDataLossYearlyCategory,
                                       treeCoverLossIDNLandCoverYearly: ForestChangeDiagnosticDataLossYearlyCategory,
                                       treeCoverLossSoyPlanedAreasYearly: ForestChangeDiagnosticDataLossYearly,
                                       treeCoverLossIDNForestAreaYearly: ForestChangeDiagnosticDataLossYearlyCategory,
                                       treeCoverLossIDNForestMoratoriumYearly: ForestChangeDiagnosticDataLossYearly,
                                       prodesLossYearly: ForestChangeDiagnosticDataLossYearly,
                                       prodesLossProtectedAreasYearly: ForestChangeDiagnosticDataLossYearly,
                                       prodesLossProdesPrimaryForestYearly: ForestChangeDiagnosticDataLossYearly,
                                       treeCoverLossBRABiomesYearly: ForestChangeDiagnosticDataLossYearlyCategory,
                                       treeCoverExtent: ForestChangeDiagnosticDataDouble,
                                       treeCoverExtentPrimaryForest: ForestChangeDiagnosticDataDouble,
                                       treeCoverExtentProtectedAreas: ForestChangeDiagnosticDataDouble,
                                       treeCoverExtentPeatlands: ForestChangeDiagnosticDataDouble,
                                       treeCoverExtentIntactForests: ForestChangeDiagnosticDataDouble,
) {

  def merge(other: ForestChangeDiagnosticData): ForestChangeDiagnosticData = {

    ForestChangeDiagnosticData(
      treeCoverLossYearly.merge(other.treeCoverLossYearly),
      treeCoverLossPrimaryForestYearly.merge(
        other.treeCoverLossPrimaryForestYearly
      ),
      treeCoverLossPeatLandYearly.merge(other.treeCoverLossPeatLandYearly),
      treeCoverLossIntactForestYearly.merge(
        other.treeCoverLossIntactForestYearly
      ),
      treeCoverLossProtectedAreasYearly.merge(
        other.treeCoverLossProtectedAreasYearly
      ),
      treeCoverLossSEAsiaLandCoverYearly.merge(
        other.treeCoverLossSEAsiaLandCoverYearly
      ),
      treeCoverLossIDNLandCoverYearly.merge(
        other.treeCoverLossIDNLandCoverYearly
      ),
      treeCoverLossSoyPlanedAreasYearly.merge(
        other.treeCoverLossSoyPlanedAreasYearly
      ),
      treeCoverLossIDNForestAreaYearly.merge(
        other.treeCoverLossIDNForestAreaYearly
      ),
      treeCoverLossIDNForestMoratoriumYearly.merge(
        other.treeCoverLossIDNForestMoratoriumYearly
      ),
      prodesLossYearly.merge(other.prodesLossYearly),
      prodesLossProtectedAreasYearly.merge(
        other.prodesLossProtectedAreasYearly
      ),
      prodesLossProdesPrimaryForestYearly.merge(
        other.prodesLossProdesPrimaryForestYearly
      ),
      treeCoverLossBRABiomesYearly.merge(other.treeCoverLossBRABiomesYearly),
      treeCoverExtent.merge(other.treeCoverExtent),
      treeCoverExtentPrimaryForest.merge(other.treeCoverExtentPrimaryForest),
      treeCoverExtentProtectedAreas.merge(other.treeCoverExtentProtectedAreas),
      treeCoverExtentPeatlands.merge(other.treeCoverExtentPeatlands),
      treeCoverExtentIntactForests.merge(other.treeCoverExtentIntactForests)
    )
  }
}

object ForestChangeDiagnosticData {

  def empty: ForestChangeDiagnosticData =
    ForestChangeDiagnosticData(
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearlyCategory.empty,
      ForestChangeDiagnosticDataLossYearlyCategory.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearlyCategory.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearlyCategory.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDouble.empty
    )

  implicit val lossDataSemigroup: Semigroup[ForestChangeDiagnosticData] =
    new Semigroup[ForestChangeDiagnosticData] {
      def combine(x: ForestChangeDiagnosticData,
                  y: ForestChangeDiagnosticData): ForestChangeDiagnosticData =
        x.merge(y)
    }

}
