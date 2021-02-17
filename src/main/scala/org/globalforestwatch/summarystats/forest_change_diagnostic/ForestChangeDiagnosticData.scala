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
                                       primaryForestArea: ForestChangeDiagnosticDataDouble,
                                       intactForest2016Area: ForestChangeDiagnosticDataDouble,
                                       totalArea: ForestChangeDiagnosticDataDouble,
                                       protectedAreasArea: ForestChangeDiagnosticDataDouble,
                                       peatlandsArea: ForestChangeDiagnosticDataDouble,
                                       braBiomesArea: ForestChangeDiagnosticDataDoubleCategory,
                                       idnForestAreaArea: ForestChangeDiagnosticDataDoubleCategory,
                                       seAsiaLandCoverArea: ForestChangeDiagnosticDataDoubleCategory,
                                       idnLandCoverArea: ForestChangeDiagnosticDataDoubleCategory,
                                       idnForestMoratoriumArea: ForestChangeDiagnosticDataDouble,
                                       southAmericaPresence: ForestChangeDiagnosticDataBoolean,
                                       legalAmazonPresence: ForestChangeDiagnosticDataBoolean,
                                       braBiomesPresence: ForestChangeDiagnosticDataBoolean,
                                       cerradoBiomesPresence: ForestChangeDiagnosticDataBoolean,
                                       seAsiaPresence: ForestChangeDiagnosticDataBoolean,
                                       idnPresence: ForestChangeDiagnosticDataBoolean

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
      treeCoverExtentIntactForests.merge(other.treeCoverExtentIntactForests),
      primaryForestArea.merge(other.primaryForestArea),
      intactForest2016Area.merge(other.intactForest2016Area),
      totalArea.merge(other.totalArea),
      protectedAreasArea.merge(other.protectedAreasArea),
      peatlandsArea.merge(other.peatlandsArea),
      braBiomesArea.merge(other.braBiomesArea),
      idnForestAreaArea.merge(other.idnForestAreaArea),
      seAsiaLandCoverArea.merge(other.seAsiaLandCoverArea),
      idnLandCoverArea.merge(other.idnLandCoverArea),
      idnForestMoratoriumArea.merge(other.idnForestMoratoriumArea),

      southAmericaPresence.merge(southAmericaPresence),
      legalAmazonPresence.merge(legalAmazonPresence),
      braBiomesPresence.merge(braBiomesPresence),
      cerradoBiomesPresence.merge(cerradoBiomesPresence),
      seAsiaPresence.merge(seAsiaPresence),
      idnPresence.merge(idnPresence)
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
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDoubleCategory.empty,
      ForestChangeDiagnosticDataDoubleCategory.empty,
      ForestChangeDiagnosticDataDoubleCategory.empty,
      ForestChangeDiagnosticDataDoubleCategory.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataBoolean.empty,
      ForestChangeDiagnosticDataBoolean.empty,
      ForestChangeDiagnosticDataBoolean.empty,
      ForestChangeDiagnosticDataBoolean.empty,
      ForestChangeDiagnosticDataBoolean.empty,
      ForestChangeDiagnosticDataBoolean.empty
    )

  implicit val lossDataSemigroup: Semigroup[ForestChangeDiagnosticData] =
    new Semigroup[ForestChangeDiagnosticData] {
      def combine(x: ForestChangeDiagnosticData,
                  y: ForestChangeDiagnosticData): ForestChangeDiagnosticData =
        x.merge(y)
    }

}
