package org.globalforestwatch.summarystats.carbonflux_minimal

import geotrellis.raster.Raster
import geotrellis.vector.Extent
import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import org.globalforestwatch.grids.{GridSources, GridTile}
import org.globalforestwatch.layers._

/**
  * @param gridTile top left corner, padded from east ex: "10N_010E"
  */
case class CarbonFluxMinimalGridSources(gridTile: GridTile) extends GridSources {

  val treeCoverLoss = TreeCoverLoss(gridTile)
  val treeCoverGain = TreeCoverGain(gridTile)
  val treeCoverDensity2000 = TreeCoverDensity2000(gridTile)   // Same as treecoverloss package
  val treeCoverDensity2010 = TreeCoverDensity2010(gridTile)
  val biomassPerHectar = BiomassPerHectar(gridTile)
  val primaryForest = PrimaryForest(gridTile)
  val plantationsBool = PlantationsBool(gridTile)

  val grossCumulAbovegroundRemovalsCo2 = GrossCumulAbovegroundRemovalsCo2(gridTile)
  val grossCumulBelowgroundRemovalsCo2 = GrossCumulBelowgroundRemovalsCo2(gridTile)
  val grossEmissionsCo2eNonCo2 = GrossEmissionsNonCo2Co2e(gridTile)
  val grossEmissionsCo2eCo2Only = GrossEmissionsCo2OnlyCo2e(gridTile)
  val netFluxCo2 = NetFluxCo2e(gridTile)
  val fluxModelExtent = FluxModelExtent(gridTile)


  def readWindow(windowKey: SpatialKey, windowLayout: LayoutDefinition): Either[Throwable, Raster[CarbonFluxMinimalTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      lossTile <- Either.catchNonFatal(treeCoverLoss.fetchWindow(windowKey, windowLayout)).right
      gainTile <- Either.catchNonFatal(treeCoverGain.fetchWindow(windowKey, windowLayout)).right
      tcd2000Tile <- Either
        .catchNonFatal(treeCoverDensity2000.fetchWindow(windowKey, windowLayout))
        .right
      tcd2010Tile <- Either
        .catchNonFatal(treeCoverDensity2010.fetchWindow(windowKey, windowLayout))
        .right

    } yield {
      // Failure for these will be converted to optional result and propagated with TreeLossTile
      val biomassTile = biomassPerHectar.fetchWindow(windowKey, windowLayout)
      val primaryForestTile = primaryForest.fetchWindow(windowKey, windowLayout)
      val plantationsBoolTile = plantationsBool.fetchWindow(windowKey, windowLayout)

      val grossCumulAbovegroundRemovalsCo2Tile = grossCumulAbovegroundRemovalsCo2.fetchWindow(windowKey, windowLayout)
      val grossCumulBelowgroundRemovalsCo2Tile = grossCumulBelowgroundRemovalsCo2.fetchWindow(windowKey, windowLayout)
      val grossEmissionsCo2eNonCo2Tile = grossEmissionsCo2eNonCo2.fetchWindow(windowKey, windowLayout)
      val grossEmissionsCo2eCo2OnlyTile = grossEmissionsCo2eCo2Only.fetchWindow(windowKey, windowLayout)
      val netFluxCo2Tile = netFluxCo2.fetchWindow(windowKey, windowLayout)
      val fluxModelExtentTile = fluxModelExtent.fetchWindow(windowKey, windowLayout)


      val tile = CarbonFluxMinimalTile(
        lossTile,
        gainTile,
        tcd2000Tile,
        tcd2010Tile,
        biomassTile,
        primaryForestTile,
        plantationsBoolTile,

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

object CarbonFluxMinimalGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap.empty[String, CarbonFluxMinimalGridSources]

  def getCachedSources(gridTile: GridTile): CarbonFluxMinimalGridSources = {

    cache.getOrElseUpdate(gridTile.tileId, CarbonFluxMinimalGridSources(gridTile))

  }

}
