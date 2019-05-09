package org.globalforestwatch.gladalerts

case class GladAlertsRow(lossRowFeatureId: GladAlertsRowFeatureId,
                         lat: Double,
                         lon: Double,
                         climateMask: Boolean,
                         totalArea: Double,
                         totalBiomass: Double,
                         totalCo2: Double)
