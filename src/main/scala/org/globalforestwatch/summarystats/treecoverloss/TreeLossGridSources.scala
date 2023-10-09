package org.globalforestwatch.summarystats.treecoverloss

import geotrellis.raster.Raster
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import org.globalforestwatch.grids.{GridSources, GridTile}
import org.globalforestwatch.layers._

/**
  * @param gridTile top left corner, padded from east ex: "10N_010E"
  */
case class TreeLossGridSources(gridTile: GridTile, kwargs: Map[String, Any]) extends GridSources {
  val treeCoverLoss = TreeCoverLoss(gridTile, kwargs)
  val treeCoverGain = TreeCoverGain(gridTile, kwargs)
  val treeCoverDensity2000 = TreeCoverDensityPercent2000(gridTile, kwargs)
  val treeCoverDensity2010 = TreeCoverDensityPercent2010(gridTile, kwargs)
  val biomassPerHectar = BiomassPerHectar(gridTile, kwargs)
  val agc2000 = Agc2000(gridTile, kwargs = kwargs)
  val bgc2000 = Bgc2000(gridTile, kwargs = kwargs)
  val soilCarbon2000 = SoilCarbon2000(gridTile, kwargs = kwargs)

  val primaryForest = PrimaryForest(gridTile, kwargs)
  val plantedForestsBool = PlantedForestsBool(gridTile, kwargs)
  val globalPeat = Peatlands(gridTile, kwargs)

  val plantationsPre2000: PlantationsPre2000 = PlantationsPre2000(gridTile, kwargs)
  val mangroveBiomassExtent: MangroveBiomassExtent = MangroveBiomassExtent(gridTile, kwargs)

  val grossCumulAbovegroundRemovalsCo2: GrossCumulAbovegroundRemovalsCo2 = GrossCumulAbovegroundRemovalsCo2(gridTile, kwargs = kwargs)
  val grossCumulBelowgroundRemovalsCo2: GrossCumulBelowgroundRemovalsCo2 = GrossCumulBelowgroundRemovalsCo2(gridTile, kwargs = kwargs)
  val grossEmissionsCo2eNonCo2: GrossEmissionsNonCo2Co2e = GrossEmissionsNonCo2Co2e(gridTile, kwargs = kwargs)
  val grossEmissionsCo2eCo2Only: GrossEmissionsCo2OnlyCo2e = GrossEmissionsCo2OnlyCo2e(gridTile, kwargs = kwargs)
  val netFluxCo2e: NetFluxCo2e = NetFluxCo2e(gridTile, kwargs = kwargs)
  val fluxModelExtent: FluxModelExtent = FluxModelExtent(gridTile, kwargs = kwargs)


  def readWindow(windowKey: SpatialKey, windowLayout: LayoutDefinition):
  Either[Throwable, Raster[TreeLossTile]] = {

    val tile = TreeLossTile(
      windowKey, windowLayout, this
    )
    Right(Raster(tile, windowKey.extent(windowLayout)))
  }
}

object TreeLossGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap.empty[String, TreeLossGridSources]

  def getCachedSources(gridTile: GridTile, kwargs: Map[String, Any]): TreeLossGridSources = {

    cache.getOrElseUpdate(gridTile.tileId, TreeLossGridSources(gridTile, kwargs))

  }

}
