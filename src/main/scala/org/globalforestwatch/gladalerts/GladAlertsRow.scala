package org.globalforestwatch.gladalerts

import org.globalforestwatch.features.FeatureId

case class GladAlertsRow(id: FeatureId,
                         alertDate: String,
                         isConfirmed: Option[Boolean],
                         x: Int,
                         y: Int,
                         z: Int,
                         layers: GladAlertsRowLayers,
                         totalAlerts: Int,
                         alertArea: Double,
                         co2Emissions: Double,
                         totalArea: Double)
