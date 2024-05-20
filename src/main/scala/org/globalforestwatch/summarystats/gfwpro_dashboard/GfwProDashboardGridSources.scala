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
  val sbtnNaturalForest: SBTNNaturalForests = SBTNNaturalForests(gridTile, kwargs)
  val jrcForestCover: JRCForestCover = JRCForestCover(gridTile, kwargs)
  val gadmAdm0: GadmAdm0 = GadmAdm0(gridTile, kwargs)
  val gadmAdm1: GadmAdm1 = GadmAdm1(gridTile, kwargs)
  val gadmAdm2: GadmAdm2 = GadmAdm2(gridTile, kwargs)

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
      sbtnNaturalForestTile <- Either
        .catchNonFatal(sbtnNaturalForest.fetchWindow(windowKey, windowLayout))
        .right
      jrcForestCoverTile <- Either
        .catchNonFatal(jrcForestCover.fetchWindow(windowKey, windowLayout))
        .right
      gadm0Tile <- Either
        .catchNonFatal(gadmAdm0.fetchWindow(windowKey, windowLayout))
        .right
      gadm1Tile <- Either
        .catchNonFatal(gadmAdm1.fetchWindow(windowKey, windowLayout))
        .right
      gadm2Tile <- Either
        .catchNonFatal(gadmAdm2.fetchWindow(windowKey, windowLayout))
        .right
    } yield {
      val tile = GfwProDashboardTile(integratedAlertsTile, tcd2000Tile, sbtnNaturalForestTile, jrcForestCoverTile, gadm0Tile, gadm1Tile, gadm2Tile)
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
