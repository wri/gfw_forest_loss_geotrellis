package org.globalforestwatch.gladalerts

case class GladAlertsRow(iso: String,
                         adm1: Integer,
                         adm2: Integer,
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
