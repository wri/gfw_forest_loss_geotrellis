package org.globalforestwatch.summarystats.treecoverloss

import geotrellis.raster.Raster
import geotrellis.vector.Extent
import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import org.globalforestwatch.grids.{GridSources, GridTile}
import org.globalforestwatch.layers._

/**
  * @param gridTile top left corner, padded from east ex: "10N_010E"
  */
case class TreeLossGridSources(gridTile: GridTile) extends GridSources {

  val treeCoverLoss = TreeCoverLoss(gridTile)
  val treeCoverGain = TreeCoverGain(gridTile)
  val treeCoverDensity2000 = TreeCoverDensityPercent2000(gridTile)
  val treeCoverDensity2010 = TreeCoverDensityPercent2010(gridTile)
  val biomassPerHectar = BiomassPerHectar(gridTile)
  val agc2000 = Agc2000(gridTile)
  val bgc2000 = Bgc2000(gridTile)
  val soilCarbon2000 = SoilCarbon2000(gridTile)

  val primaryForest = PrimaryForest(gridTile)
  val plantationsBool = PlantationsBool(gridTile)

  val grossCumulAbovegroundRemovalsCo2 = GrossCumulAbovegroundRemovalsCo2(gridTile)
  val grossCumulBelowgroundRemovalsCo2 = GrossCumulBelowgroundRemovalsCo2(gridTile)
  val grossEmissionsCo2eNonCo2 = GrossEmissionsNonCo2Co2e(gridTile)
  val grossEmissionsCo2eCo2Only = GrossEmissionsCo2OnlyCo2e(gridTile)
  val netFluxCo2 = NetFluxCo2e(gridTile)
  val fluxModelExtent = FluxModelExtent(gridTile)


  def readWindow(windowKey: SpatialKey, windowLayout: LayoutDefinition): Either[Throwable, Raster[TreeLossTile]] = {

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
      val primaryForestTile = primaryForest.fetchWindow(windowKey, windowLayout)
      val plantationsBoolTile = plantationsBool.fetchWindow(windowKey, windowLayout)

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

object TreeLossGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap.empty[String, TreeLossGridSources]

  def getCachedSources(gridTile: GridTile): TreeLossGridSources = {

    cache.getOrElseUpdate(gridTile.tileId, TreeLossGridSources(gridTile))

  }

}
