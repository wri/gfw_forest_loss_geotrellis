package org.globalforestwatch.summarystats.gfwpro_dashboard

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

    val tile = GfwProDashboardTile(
      windowKey, windowLayout, this
    )
    Right(Raster(tile, windowKey.extent(windowLayout)))
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
