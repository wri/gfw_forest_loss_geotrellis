package org.globalforestwatch.gladalerts

import org.globalforestwatch.features.WdpaFeatureId

case class GladAlertsRowWdpa(id: WdpaFeatureId,
                             dataGroup: GladAlertsDataGroup,
                             data: GladAlertsData)
//                         alertDate: String,
//                         isConfirmed: Option[Boolean],
//                         x: Int,
//                         y: Int,
//                         z: Int,
//                         layers: GladAlertsRowLayers,
//                         totalAlerts: Int,
//                         alertArea: Double,
//                         co2Emissions: Double,
//                         totalArea: Double)
