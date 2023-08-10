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
        AFiDataDouble.fill(totalArea, negligibleRisk != "Y"),
        AFiDataDouble.fill(totalArea)
      )
  }
}
