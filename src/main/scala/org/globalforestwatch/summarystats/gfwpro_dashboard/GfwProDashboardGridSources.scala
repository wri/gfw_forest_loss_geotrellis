package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.Raster
import org.globalforestwatch.grids.{GridSources, GridTile}
import org.globalforestwatch.layers._

/**
  * @param gridTile top left corner, padded from east ex: "10N_010E"
  */
case class GfwProDashboardGridSources(gridTile: GridTile) extends GridSources {

  val gladAlerts = GladAlerts(gridTile)

  def readWindow(
    windowKey: SpatialKey,
    windowLayout: LayoutDefinition
  ): Either[Throwable, Raster[GfwProDashboardTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      gladAlertsTile <- Either
        .catchNonFatal(gladAlerts.fetchWindow(windowKey, windowLayout))
        .right

    } yield {

      val tile = GfwProDashboardTile(gladAlertsTile, )

      Raster(tile, windowKey.extent(windowLayout))
    }
  }
}

object GfwProDashboardGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap
      .empty[String, GfwProDashboardGridSources]

  def getCachedSources(gridTile: GridTile): GfwProDashboardGridSources = {

    cache.getOrElseUpdate(gridTile.tileId, GfwProDashboardGridSources(gridTile))

  }

}
