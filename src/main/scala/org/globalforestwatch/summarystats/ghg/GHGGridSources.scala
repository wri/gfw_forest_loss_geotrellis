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
  val grossEmissionsCo2eCo2Only: GrossEmissionsCo2OnlyCo2BiomassSoil = GrossEmissionsCo2OnlyCo2BiomassSoil(gridTile, kwargs = kwargs)
  val grossEmissionsCo2eCH4: GrossEmissionsCH4Co2eBiomassSoil = GrossEmissionsCH4Co2eBiomassSoil(gridTile, kwargs = kwargs)
  val grossEmissionsCo2eN2O: GrossEmissionsN2OCo2eBiomassSoil = GrossEmissionsN2OCo2eBiomassSoil(gridTile, kwargs = kwargs)
  val mapspamCOCOYield: MapspamYield = MapspamYield("COCO", gridTile, kwargs = kwargs)
  val mapspamCOFFYield: MapspamYield = MapspamYield("COFF", gridTile, kwargs = kwargs)
  val mapspamOILPYield: MapspamYield = MapspamYield("OILP", gridTile, kwargs = kwargs)
  val mapspamRUBBYield: MapspamYield = MapspamYield("RUBB", gridTile, kwargs = kwargs)
  val mapspamSOYBYield: MapspamYield = MapspamYield("SOYB", gridTile, kwargs = kwargs)
  val mapspamSUGCYield: MapspamYield = MapspamYield("SUGC", gridTile, kwargs = kwargs)
  val gadmAdm0: GadmAdm0 = GadmAdm0(gridTile, kwargs)
  val gadmAdm1: GadmAdm1 = GadmAdm1(gridTile, kwargs)
  val gadmAdm2: GadmAdm2 = GadmAdm2(gridTile, kwargs)
  val treeCoverGainFromHeight: TreeCoverGainFromHeight = TreeCoverGainFromHeight(gridTile, kwargs)
  val mangroveBiomassExtent: MangroveBiomassExtent  = MangroveBiomassExtent(gridTile, kwargs)
  val plantationsPre2000: PlantationsPre2000 = PlantationsPre2000(gridTile, kwargs)

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
