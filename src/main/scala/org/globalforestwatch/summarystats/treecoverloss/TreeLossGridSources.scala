package org.globalforestwatch.summarystats.treecoverloss

import geotrellis.raster.Raster
import cats.implicits._
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
  val plantationsPre2000: PlantationsPre2000 = PlantationsPre2000(gridTile, kwargs)

  val grossCumulAbovegroundRemovalsCo2: GrossCumulAbovegroundRemovalsCo2 = GrossCumulAbovegroundRemovalsCo2(gridTile, kwargs = kwargs)
  val grossCumulBelowgroundRemovalsCo2: GrossCumulBelowgroundRemovalsCo2 = GrossCumulBelowgroundRemovalsCo2(gridTile, kwargs = kwargs)
  val grossEmissionsCo2eNonCo2: GrossEmissionsNonCo2Co2eBiomassSoil = GrossEmissionsNonCo2Co2eBiomassSoil(gridTile, kwargs = kwargs)
  val grossEmissionsCo2eCo2Only: GrossEmissionsCo2OnlyCo2eBiomassSoil = GrossEmissionsCo2OnlyCo2eBiomassSoil(gridTile, kwargs = kwargs)
  val netFluxCo2: NetFluxCo2e = NetFluxCo2e(gridTile, kwargs = kwargs)
  val fluxModelExtent: FluxModelExtent = FluxModelExtent(gridTile, kwargs = kwargs)


  def readWindow(windowKey: SpatialKey, windowLayout: LayoutDefinition): Either[Throwable, Raster[TreeLossTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      lossTile <- Either.catchNonFatal(treeCoverLoss.fetchWindow(windowKey, windowLayout)).right
      tcd2000Tile <- Either
        .catchNonFatal(treeCoverDensity2000.fetchWindow(windowKey, windowLayout))
        .right
      tcd2010Tile <- Either
        .catchNonFatal(treeCoverDensity2010.fetchWindow(windowKey, windowLayout))
        .right

    } yield {
      // Failure for these will be converted to optional result and propagated with TreeLossTile
      val gainTile = treeCoverGain.fetchWindow(windowKey, windowLayout)
      val primaryForestTile = primaryForest.fetchWindow(windowKey, windowLayout)
      val plantedForestsBoolTile = plantedForestsBool.fetchWindow(windowKey, windowLayout)
      val plantationsPre2000Tile = plantationsPre2000.fetchWindow(windowKey, windowLayout)

      val biomassTile = biomassPerHectar.fetchWindow(windowKey, windowLayout)
      val agc2000Tile = agc2000.fetchWindow(windowKey, windowLayout)
      val bgc2000Tile = bgc2000.fetchWindow(windowKey, windowLayout)
      val soilCarbon2000Tile = soilCarbon2000.fetchWindow(windowKey, windowLayout)

      val grossCumulAbovegroundRemovalsCo2Tile = grossCumulAbovegroundRemovalsCo2.fetchWindow(windowKey, windowLayout)
      val grossCumulBelowgroundRemovalsCo2Tile = grossCumulBelowgroundRemovalsCo2.fetchWindow(windowKey, windowLayout)
      val grossEmissionsCo2eNonCo2Tile = grossEmissionsCo2eNonCo2.fetchWindow(windowKey, windowLayout)
      val grossEmissionsCo2eCo2OnlyTile = grossEmissionsCo2eCo2Only.fetchWindow(windowKey, windowLayout)
      val netFluxCo2Tile = netFluxCo2.fetchWindow(windowKey, windowLayout)
      val fluxModelExtentTile = fluxModelExtent.fetchWindow(windowKey, windowLayout)

      val tile = TreeLossTile(
        lossTile,
        gainTile,
        tcd2000Tile,
        tcd2010Tile,
        biomassTile,
        agc2000Tile,
        bgc2000Tile,
        soilCarbon2000Tile,
        primaryForestTile,
        plantedForestsBoolTile,
        plantationsPre2000Tile,
        grossCumulAbovegroundRemovalsCo2Tile,
        grossCumulBelowgroundRemovalsCo2Tile,
        netFluxCo2Tile,
        grossEmissionsCo2eNonCo2Tile,
        grossEmissionsCo2eCo2OnlyTile,
        fluxModelExtentTile
      )

      Raster(tile, windowKey.extent(windowLayout))
    }
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
