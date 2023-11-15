package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.Raster
import org.globalforestwatch.grids.{GridSources, GridTile}
import org.globalforestwatch.layers._

/**
  * @param gridTile top left corner, padded from east ex: "10N_010E"
  */
case class GfwProDashboardGridSources(gridTile: GridTile, kwargs: Map[String, Any]) extends GridSources {
  val integratedAlerts = IntegratedAlerts(gridTile, kwargs)
  val treeCoverDensity2000 = TreeCoverDensityPercent2000(gridTile, kwargs)

  def readWindow(
                  windowKey: SpatialKey,
                  windowLayout: LayoutDefinition
                ): Either[Throwable, Raster[GfwProDashboardTile]] = {

    for {
      // Integrated alerts are Optional Tiles, but we keep it this way to avoid signature changes
      integratedAlertsTile <- Either
        .catchNonFatal(integratedAlerts.fetchWindow(windowKey, windowLayout))
        .right
      tcd2000Tile <- Either
        .catchNonFatal(treeCoverDensity2000.fetchWindow(windowKey, windowLayout))
        .right
    } yield {
      val tile = GfwProDashboardTile(integratedAlertsTile, tcd2000Tile)
      Raster(tile, windowKey.extent(windowLayout))
    }
  }
}

object GfwProDashboardGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap
      .empty[String, GfwProDashboardGridSources]

  def getCachedSources(gridTile: GridTile, kwargs: Map[String, Any]): GfwProDashboardGridSources = {
    cache.getOrElseUpdate(gridTile.tileId, GfwProDashboardGridSources(gridTile, kwargs))
  }
}
