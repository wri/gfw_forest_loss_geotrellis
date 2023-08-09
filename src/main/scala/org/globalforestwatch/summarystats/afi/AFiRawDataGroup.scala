package org.globalforestwatch.summarystats.afi

import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticDataDouble
import java.time.LocalDate

case class AFiRawDataGroup(
  treeCoverLossYear: Int,
  gadmId: String

) {
    def toAFiData(treeCoverLossArea: Double): AFiData = {
      AFiData(AFiDataLossYearly.fill(treeCoverLossYear, treeCoverLossArea))
  }
}
