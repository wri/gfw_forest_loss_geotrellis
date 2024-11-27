package org.globalforestwatch.summarystats.ghg

import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.Raster
import org.globalforestwatch.grids.{GridSources, GridTile}
import org.globalforestwatch.layers._

/**
  * @param gridTile top left corner, padded from east ex: "10N_010E"
  */
case class GHGGridSources(gridTile: GridTile, kwargs: Map[String, Any])
  extends GridSources {

  val treeCoverLoss: TreeCoverLoss = TreeCoverLoss(gridTile, kwargs)
  val treeCoverDensity2000: TreeCoverDensityPercent2000 = TreeCoverDensityPercent2000(gridTile, kwargs)
  val grossEmissionsCo2eNonCo2: GrossEmissionsNonCo2Co2eBiomassSoil = GrossEmissionsNonCo2Co2eBiomassSoil(gridTile, kwargs = kwargs)
  val grossEmissionsCo2eCo2Only: GrossEmissionsCo2OnlyCo2BiomassSoil = GrossEmissionsCo2OnlyCo2BiomassSoil(gridTile, kwargs = kwargs)
  val mapspamCOCOYield: MapspamCOCOYield = MapspamCOCOYield(gridTile, kwargs = kwargs)

  def readWindow(
                  windowKey: SpatialKey,
                  windowLayout: LayoutDefinition
                ): Either[Throwable, Raster[GHGTile]] = {

    val tile = GHGTile(
      windowKey, windowLayout, this
    )
    Right(Raster(tile, windowKey.extent(windowLayout)))
  }
}

object GHGGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap
      .empty[String, GHGGridSources]

  def getCachedSources(
                        gridTile: GridTile,
                        kwargs: Map[String, Any]
                      ): GHGGridSources = {

    cache.getOrElseUpdate(
      gridTile.tileId,
      GHGGridSources(gridTile, kwargs)
    )

  }

}
