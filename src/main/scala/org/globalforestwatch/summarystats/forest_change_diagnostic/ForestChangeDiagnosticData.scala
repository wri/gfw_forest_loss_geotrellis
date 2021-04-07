package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.Semigroup

/** Summary data per class
  *
  * Note: This case class contains mutable values
  */
case class ForestChangeDiagnosticData(
                                       treeCoverLossTcd30Yearly: ForestChangeDiagnosticDataLossYearly,
                                       treeCoverLossTcd90Yearly: ForestChangeDiagnosticDataLossYearly,
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
                                       idnPresence: ForestChangeDiagnosticDataBoolean,
                                       filteredTreeCoverExtent: ForestChangeDiagnosticDataDouble,
                                       filteredTreeCoverExtentYearly: ForestChangeDiagnosticDataValueYearly,
                                       filteredTreeCoverLossYearly: ForestChangeDiagnosticDataLossYearly,
                                       filteredTreeCoverLossPeatYearly: ForestChangeDiagnosticDataLossYearly,
                                       filteredTreeCoverLossProtectedAreasYearly: ForestChangeDiagnosticDataLossYearly,
                                       plantationArea: ForestChangeDiagnosticDataDouble,
                                       plantationOnPeatArea: ForestChangeDiagnosticDataDouble,
                                       plantationInProtectedAreasArea: ForestChangeDiagnosticDataDouble,
                                       forestValueIndicator: ForestChangeDiagnosticDataValueYearly,
                                       peatValueIndicator: ForestChangeDiagnosticDataValueYearly,
                                       protectedAreaValueIndicator: ForestChangeDiagnosticDataValueYearly,
                                       deforestationThreatIndicator: ForestChangeDiagnosticDataLossYearly,
                                       peatThreatIndicator: ForestChangeDiagnosticDataLossYearly,
                                       protectedAreaThreatIndicator: ForestChangeDiagnosticDataLossYearly,
                                       fireThreatIndicator: ForestChangeDiagnosticDataLossYearly
                                     ) {

  def merge(other: ForestChangeDiagnosticData): ForestChangeDiagnosticData = {

    ForestChangeDiagnosticData(
      treeCoverLossTcd30Yearly.merge(other.treeCoverLossTcd30Yearly),
      treeCoverLossTcd90Yearly.merge(other.treeCoverLossTcd90Yearly),
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
      southAmericaPresence.merge(other.southAmericaPresence),
      legalAmazonPresence.merge(other.legalAmazonPresence),
      braBiomesPresence.merge(other.braBiomesPresence),
      cerradoBiomesPresence.merge(other.cerradoBiomesPresence),
      seAsiaPresence.merge(other.seAsiaPresence),
      idnPresence.merge(other.idnPresence),
      filteredTreeCoverExtent.merge(other.filteredTreeCoverExtent),
      filteredTreeCoverExtentYearly.merge(other.filteredTreeCoverExtentYearly),
      filteredTreeCoverLossYearly.merge(other.filteredTreeCoverLossYearly),
      filteredTreeCoverLossPeatYearly.merge(
        other.filteredTreeCoverLossPeatYearly
      ),
      filteredTreeCoverLossProtectedAreasYearly.merge(
        other.filteredTreeCoverLossProtectedAreasYearly
      ),
      plantationArea.merge(other.plantationArea),
      plantationOnPeatArea.merge(other.plantationOnPeatArea),
      plantationInProtectedAreasArea.merge(
        other.plantationInProtectedAreasArea
      ),
      forestValueIndicator.merge(other.forestValueIndicator),
      peatValueIndicator.merge(other.peatValueIndicator),
      protectedAreaValueIndicator.merge(other.protectedAreaValueIndicator),
      deforestationThreatIndicator.merge(other.deforestationThreatIndicator),
      peatThreatIndicator.merge(other.peatThreatIndicator),
      protectedAreaThreatIndicator.merge(other.protectedAreaThreatIndicator),
      fireThreatIndicator.merge(other.fireThreatIndicator)
    )
  }

  def update(
              treeCoverLossTcd30Yearly: ForestChangeDiagnosticDataLossYearly =
              this.treeCoverLossTcd30Yearly,
              treeCoverLossTcd90Yearly: ForestChangeDiagnosticDataLossYearly =
              this.treeCoverLossTcd90Yearly,
              treeCoverLossPrimaryForestYearly: ForestChangeDiagnosticDataLossYearly =
              this.treeCoverLossPrimaryForestYearly,
              treeCoverLossPeatLandYearly: ForestChangeDiagnosticDataLossYearly =
              this.treeCoverLossPeatLandYearly,
              treeCoverLossIntactForestYearly: ForestChangeDiagnosticDataLossYearly =
              this.treeCoverLossProtectedAreasYearly,
              treeCoverLossProtectedAreasYearly: ForestChangeDiagnosticDataLossYearly =
              this.treeCoverLossProtectedAreasYearly,
              treeCoverLossSEAsiaLandCoverYearly: ForestChangeDiagnosticDataLossYearlyCategory =
              this.treeCoverLossSEAsiaLandCoverYearly,
              treeCoverLossIDNLandCoverYearly: ForestChangeDiagnosticDataLossYearlyCategory =
              this.treeCoverLossIDNLandCoverYearly,
              treeCoverLossSoyPlanedAreasYearly: ForestChangeDiagnosticDataLossYearly =
              this.treeCoverLossSoyPlanedAreasYearly,
              treeCoverLossIDNForestAreaYearly: ForestChangeDiagnosticDataLossYearlyCategory =
              this.treeCoverLossIDNForestAreaYearly,
              treeCoverLossIDNForestMoratoriumYearly: ForestChangeDiagnosticDataLossYearly =
              this.treeCoverLossIDNForestMoratoriumYearly,
              prodesLossYearly: ForestChangeDiagnosticDataLossYearly =
              this.prodesLossYearly,
              prodesLossProtectedAreasYearly: ForestChangeDiagnosticDataLossYearly =
              this.prodesLossProtectedAreasYearly,
              prodesLossProdesPrimaryForestYearly: ForestChangeDiagnosticDataLossYearly =
              this.prodesLossProdesPrimaryForestYearly,
              treeCoverLossBRABiomesYearly: ForestChangeDiagnosticDataLossYearlyCategory =
              this.treeCoverLossBRABiomesYearly,
              treeCoverExtent: ForestChangeDiagnosticDataDouble = this.treeCoverExtent,
              treeCoverExtentPrimaryForest: ForestChangeDiagnosticDataDouble =
              this.treeCoverExtentPrimaryForest,
              treeCoverExtentProtectedAreas: ForestChangeDiagnosticDataDouble =
              this.treeCoverExtentProtectedAreas,
              treeCoverExtentPeatlands: ForestChangeDiagnosticDataDouble =
              this.treeCoverExtentPeatlands,
              treeCoverExtentIntactForests: ForestChangeDiagnosticDataDouble =
              this.treeCoverExtentIntactForests,
              primaryForestArea: ForestChangeDiagnosticDataDouble = this.primaryForestArea,
              intactForest2016Area: ForestChangeDiagnosticDataDouble =
              this.intactForest2016Area,
              totalArea: ForestChangeDiagnosticDataDouble = this.totalArea,
              protectedAreasArea: ForestChangeDiagnosticDataDouble =
              this.protectedAreasArea,
              peatlandsArea: ForestChangeDiagnosticDataDouble = this.peatlandsArea,
              braBiomesArea: ForestChangeDiagnosticDataDoubleCategory = this.braBiomesArea,
              idnForestAreaArea: ForestChangeDiagnosticDataDoubleCategory =
              this.idnForestAreaArea,
              seAsiaLandCoverArea: ForestChangeDiagnosticDataDoubleCategory =
              this.seAsiaLandCoverArea,
              idnLandCoverArea: ForestChangeDiagnosticDataDoubleCategory =
              this.idnLandCoverArea,
              idnForestMoratoriumArea: ForestChangeDiagnosticDataDouble =
              this.idnForestMoratoriumArea,
              southAmericaPresence: ForestChangeDiagnosticDataBoolean =
              this.southAmericaPresence,
              legalAmazonPresence: ForestChangeDiagnosticDataBoolean =
              this.legalAmazonPresence,
              braBiomesPresence: ForestChangeDiagnosticDataBoolean =
              this.braBiomesPresence,
              cerradoBiomesPresence: ForestChangeDiagnosticDataBoolean =
              this.cerradoBiomesPresence,
              seAsiaPresence: ForestChangeDiagnosticDataBoolean = this.seAsiaPresence,
              idnPresence: ForestChangeDiagnosticDataBoolean = this.idnPresence,
              filteredTreeCoverExtent: ForestChangeDiagnosticDataDouble = this.filteredTreeCoverExtent,
              filteredTreeCoverExtentYearly: ForestChangeDiagnosticDataValueYearly =
              this.filteredTreeCoverExtentYearly,
              filteredTreeCoverLossYearly: ForestChangeDiagnosticDataLossYearly =
              this.filteredTreeCoverLossYearly,
              filteredTreeCoverLossPeatYearly: ForestChangeDiagnosticDataLossYearly =
              this.filteredTreeCoverLossPeatYearly,
              filteredTreeCoverLossProtectedAreasYearly: ForestChangeDiagnosticDataLossYearly =
              this.filteredTreeCoverLossProtectedAreasYearly,
              plantationArea: ForestChangeDiagnosticDataDouble = this.plantationArea,
              plantationOnPeatArea: ForestChangeDiagnosticDataDouble =
              this.plantationOnPeatArea,
              plantationInProtectedAreasArea: ForestChangeDiagnosticDataDouble =
              this.plantationInProtectedAreasArea,
              forestValueIndicator: ForestChangeDiagnosticDataValueYearly =
              this.forestValueIndicator,
              peatValueIndicator: ForestChangeDiagnosticDataValueYearly =
              this.peatValueIndicator,
              protectedAreaValueIndicator: ForestChangeDiagnosticDataValueYearly =
              this.protectedAreaValueIndicator,
              deforestationThreatIndicator: ForestChangeDiagnosticDataLossYearly =
              this.deforestationThreatIndicator,
              peatThreatIndicator: ForestChangeDiagnosticDataLossYearly =
              this.peatThreatIndicator,
              protectedAreaThreatIndicator: ForestChangeDiagnosticDataLossYearly =
              this.protectedAreaThreatIndicator,
              fireThreatIndicator: ForestChangeDiagnosticDataLossYearly =
              this.fireThreatIndicator
            ): ForestChangeDiagnosticData = {

    ForestChangeDiagnosticData(
      treeCoverLossTcd30Yearly,
      treeCoverLossTcd90Yearly,
      treeCoverLossPrimaryForestYearly,
      treeCoverLossPeatLandYearly,
      treeCoverLossIntactForestYearly,
      treeCoverLossProtectedAreasYearly,
      treeCoverLossSEAsiaLandCoverYearly,
      treeCoverLossIDNLandCoverYearly,
      treeCoverLossSoyPlanedAreasYearly,
      treeCoverLossIDNForestAreaYearly,
      treeCoverLossIDNForestMoratoriumYearly,
      prodesLossYearly,
      prodesLossProtectedAreasYearly,
      prodesLossProdesPrimaryForestYearly,
      treeCoverLossBRABiomesYearly,
      treeCoverExtent,
      treeCoverExtentPrimaryForest,
      treeCoverExtentProtectedAreas,
      treeCoverExtentPeatlands,
      treeCoverExtentIntactForests,
      primaryForestArea,
      intactForest2016Area,
      totalArea,
      protectedAreasArea,
      peatlandsArea,
      braBiomesArea,
      idnForestAreaArea,
      seAsiaLandCoverArea,
      idnLandCoverArea,
      idnForestMoratoriumArea,
      southAmericaPresence,
      legalAmazonPresence,
      braBiomesPresence,
      cerradoBiomesPresence,
      seAsiaPresence,
      idnPresence,
      filteredTreeCoverExtent,
      filteredTreeCoverExtentYearly,
      filteredTreeCoverLossYearly,
      filteredTreeCoverLossPeatYearly,
      filteredTreeCoverLossProtectedAreasYearly,
      plantationArea,
      plantationOnPeatArea,
      plantationInProtectedAreasArea,
      forestValueIndicator,
      peatValueIndicator,
      protectedAreaValueIndicator,
      deforestationThreatIndicator,
      peatThreatIndicator,
      protectedAreaThreatIndicator,
      fireThreatIndicator
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
      ForestChangeDiagnosticDataBoolean.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataValueYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataDouble.empty,
      ForestChangeDiagnosticDataValueYearly.empty,
      ForestChangeDiagnosticDataValueYearly.empty,
      ForestChangeDiagnosticDataValueYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
      ForestChangeDiagnosticDataLossYearly.empty,
    )

  implicit val lossDataSemigroup: Semigroup[ForestChangeDiagnosticData] =
    new Semigroup[ForestChangeDiagnosticData] {
      def combine(x: ForestChangeDiagnosticData,
                  y: ForestChangeDiagnosticData): ForestChangeDiagnosticData =
        x.merge(y)
    }

}
