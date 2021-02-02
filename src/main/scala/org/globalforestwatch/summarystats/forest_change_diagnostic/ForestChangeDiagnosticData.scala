package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class ForestChangeDiagnosticData(
                                       treeCoverLossTotalYearly: ForestChangeDiagnosticLossYearly,
                                       treeCoverLossPrimaryForestYearly: ForestChangeDiagnosticLossYearly,
                                       treeCoverLossPeatLandYearly: ForestChangeDiagnosticLossYearly,
                                       treeCoverLossIntactForestYearly: ForestChangeDiagnosticLossYearly,
                                       treeCoverLossProtectedAreasYearly: ForestChangeDiagnosticLossYearly,
                                       treeCoverLossSEAsiaLandCoverYearly: ForestChangeDiagnosticLossYearlyCategory,
                                       treeCoverLossIDNLandCoverYearly: ForestChangeDiagnosticLossYearlyCategory,
                                       treeCoverLossSoyPlanedAreasYearly: ForestChangeDiagnosticLossYearly,
                                       treeCoverLossIDNForestAreaYearly: ForestChangeDiagnosticLossYearlyCategory,
                                       treeCoverLossIDNForestMoratoriumYearly: ForestChangeDiagnosticLossYearly,
                                       prodesLossYearly: ForestChangeDiagnosticLossYearly,
                                       prodesLossProtectedAreasYearly: ForestChangeDiagnosticLossYearly,
                                       prodesLossProdesPrimaryForestYearly: ForestChangeDiagnosticLossYearly,
                                       treeCoverLossBRABiomesYearly: ForestChangeDiagnosticLossYearlyCategory

) {

  def merge(other: ForestChangeDiagnosticData): ForestChangeDiagnosticData = {

    ForestChangeDiagnosticData(
      treeCoverLossTotalYearly.merge(other.treeCoverLossTotalYearly),
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
      prodesLossProtectedAreasYearly.merge(other.prodesLossProtectedAreasYearly),
      prodesLossProdesPrimaryForestYearly.merge(other.prodesLossProdesPrimaryForestYearly),
      treeCoverLossBRABiomesYearly.merge(other.treeCoverLossBRABiomesYearly)
    )
  }
}

object ForestChangeDiagnosticData {

  def empty: ForestChangeDiagnosticData = ForestChangeDiagnosticData(
    ForestChangeDiagnosticLossYearly.empty,
    ForestChangeDiagnosticLossYearly.empty,
    ForestChangeDiagnosticLossYearly.empty,
    ForestChangeDiagnosticLossYearly.empty,
    ForestChangeDiagnosticLossYearly.empty,
    ForestChangeDiagnosticLossYearlyCategory.empty,
    ForestChangeDiagnosticLossYearlyCategory.empty,
    ForestChangeDiagnosticLossYearly.empty,
    ForestChangeDiagnosticLossYearlyCategory.empty,
    ForestChangeDiagnosticLossYearly.empty,
    ForestChangeDiagnosticLossYearly.empty,
    ForestChangeDiagnosticLossYearly.empty,
    ForestChangeDiagnosticLossYearly.empty,
    ForestChangeDiagnosticLossYearlyCategory.empty)

  implicit val lossDataSemigroup: Semigroup[ForestChangeDiagnosticData] =
    new Semigroup[ForestChangeDiagnosticData] {
      def combine(x: ForestChangeDiagnosticData,
                  y: ForestChangeDiagnosticData): ForestChangeDiagnosticData =
        x.merge(y)
    }

}
