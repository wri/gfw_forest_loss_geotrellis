package org.globalforestwatch.summarystats.afi

import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticDataDouble

import java.time.LocalDate

case class AFiRawDataGroup(
  treeCoverLossYear: Int,
  gadmId: String,
  isNaturalLand: Boolean,
  negligibleRisk: String,
) {
    def toAFiData(totalArea: Double): AFiData = {
      AFiData(
        AFiDataLossYearly.fill(treeCoverLossYear, totalArea, isNaturalLand),
        AFiDataDouble.fill(totalArea, isNaturalLand),
        AFiDataBoolean.fill(negligibleRisk == "YES"),
        AFiDataDoubleCategory.fill(gadmId, totalArea, include = negligibleRisk != "NA"),
        AFiDataDoubleCategory.fill(gadmId, totalArea)
      )
  }
}
