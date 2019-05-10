package org.globalforestwatch.gladalerts

import java.time.LocalDate

case class GladAlertsRow(iso: String,
                         adm1: Integer,
                         adm2: Integer,
                         alertDate: String,
                         isConfirmed: Boolean,
                         x: Int,
                         y: Int,
                         z: Int,
                         climateMask: Boolean,
                         totalAlerts: Int,
                         totalArea: Double,
                         totalCo2: Double)
