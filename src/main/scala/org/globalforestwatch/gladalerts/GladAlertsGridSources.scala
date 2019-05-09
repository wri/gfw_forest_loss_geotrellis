package org.globalforestwatch.gladalerts

import cats.implicits._
import geotrellis.raster.Raster
import geotrellis.vector.Extent
import org.globalforestwatch.layers._
import org.globalforestwatch.util.TenByTenGridSources

/**
  * @param grid top left corner, padded from east ex: "10N_010E"
  */
case class GladAlertsGridSources(grid: String) extends TenByTenGridSources {

  lazy val gladAlerts = new GladAlerts(grid)
  lazy val biomassPerHectar = new BiomassPerHectar(grid)
  lazy val climateMask = new ClimateMask(grid)

  def readWindow(window: Extent): Either[Throwable, Raster[GladAlertsTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      gladAlertsTile <- Either
        .catchNonFatal(gladAlerts.fetchWindow(window))
        .right

    } yield {
      // Failure for these will be converted to optional result and propagated with TreeLossTile
      val biomassTile = biomassPerHectar.fetchWindow(window)
      val climateMaskTile = climateMask.fetchWindow(window)

      val tile = GladAlertsTile(gladAlertsTile, biomassTile, climateMaskTile)

      Raster(tile, window)
    }
  }
}
