package org.globalforestwatch.gladalerts

import java.time.LocalDate

import org.globalforestwatch.util.Mercantile

case class GladAlertsDataGroup(alertDate: LocalDate, isConfirmed: Boolean, tile: Mercantile.Tile, climateMask: Boolean)
