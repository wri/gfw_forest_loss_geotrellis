package org.globalforestwatch.gladalerts

import org.globalforestwatch.util.Mercantile

case class GladAlertsRow(iso: String,
                         adm1: Integer,
                         adm2: Integer,
                         x: Int,
                         y: Int,
                         z: Int,
                         climateMask: Boolean,
                         totalArea: Double,
                         totalBiomass: Double,
                         totalCo2: Double)
