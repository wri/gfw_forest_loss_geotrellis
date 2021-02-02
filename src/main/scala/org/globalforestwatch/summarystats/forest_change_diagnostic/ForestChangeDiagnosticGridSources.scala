package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.Raster
import org.globalforestwatch.grids.{GridSources, GridTile}
import org.globalforestwatch.layers._

/**
  * @param gridTile top left corner, padded from east ex: "10N_010E"
  */
case class ForestChangeDiagnosticGridSources(gridTile: GridTile)
    extends GridSources {

  val treeCoverLoss = TreeCoverLoss(gridTile)
  val treeCoverDensity2000 = TreeCoverDensity2000(gridTile)
  val primaryForest = PrimaryForest(gridTile)
  val peatlands = Peatlands(gridTile)
  val intactForestLandscapes = IntactForestLandscapes(gridTile)
  val protectedAreas = ProtectedAreas(gridTile)

  def readWindow(
    windowKey: SpatialKey,
    windowLayout: LayoutDefinition
  ): Either[Throwable, Raster[ForestChangeDiagnosticTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      lossTile <- Either
        .catchNonFatal(treeCoverLoss.fetchWindow(windowKey, windowLayout))
        .right
      tcd2000Tile <- Either
        .catchNonFatal(
          treeCoverDensity2000.fetchWindow(windowKey, windowLayout)
        )
        .right

    } yield {
      // Failure for these will be converted to optional result and propagated with ForestChangeDiagnosticTile
      val primaryForestTile = primaryForest.fetchWindow(windowKey, windowLayout)
      val peatlandsTile = peatlands.fetchWindow(windowKey, windowLayout)
      val intactForestLandscapesTile =
        intactForestLandscapes.fetchWindow(windowKey, windowLayout)
      val wdpaTile = protectedAreas.fetchWindow(windowKey, windowLayout)

      val tile = ForestChangeDiagnosticTile(
        lossTile,
        tcd2000Tile,
        primaryForestTile,
        peatlandsTile,
        intactForestLandscapesTile,
        wdpaTile
      )

      Raster(tile, windowKey.extent(windowLayout))
    }
  }
}

object ForestChangeDiagnosticGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap
      .empty[String, ForestChangeDiagnosticGridSources]

  def getCachedSources(
    gridTile: GridTile
  ): ForestChangeDiagnosticGridSources = {

    cache.getOrElseUpdate(
      gridTile.tileId,
      ForestChangeDiagnosticGridSources(gridTile)
    )

  }

}
