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
  val isPrimaryForest = PrimaryForest(gridTile)
  val isPeatlands = Peatlands(gridTile)
  val isIntactForestLandscapes2016 = IntactForestLandscapes2016(gridTile)
  val protectedAreas = ProtectedAreas(gridTile)
  val seAsiaLandCover = SEAsiaLandCover(gridTile)
  val idnLandCover = IndonesiaLandCover(gridTile)
  val isSoyPlantedArea = SoyPlantedAreas(gridTile)
  val idnForestArea = IndonesiaForestArea(gridTile)
  val isIDNForestMoratorium = IndonesiaForestMoratorium(gridTile)

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
      val isPrimaryForestTile = isPrimaryForest.fetchWindow(windowKey, windowLayout)
      val isPeatlandsTile = isPeatlands.fetchWindow(windowKey, windowLayout)
      val isIntactForestLandscapes2016Tile =
        isIntactForestLandscapes2016.fetchWindow(windowKey, windowLayout)
      val wdpaTile = protectedAreas.fetchWindow(windowKey, windowLayout)
      val seAsiaLandCoverTile = seAsiaLandCover.fetchWindow(windowKey, windowLayout)
      val idnLandCoverTile = idnLandCover.fetchWindow(windowKey, windowLayout)
      val isSoyPlantedAreasTile = isSoyPlantedArea.fetchWindow(windowKey, windowLayout)
      val idnForestAreaTile = idnForestArea.fetchWindow(windowKey, windowLayout)
      val isINDForestMoratoriumTile = isIDNForestMoratorium.fetchWindow(windowKey, windowLayout)

      val tile = ForestChangeDiagnosticTile(
        lossTile,
        tcd2000Tile,
        isPrimaryForestTile,
        isPeatlandsTile,
        isIntactForestLandscapes2016Tile,
        wdpaTile,
        seAsiaLandCoverTile,
        idnLandCoverTile,
        isSoyPlantedAreasTile,
        idnForestAreaTile,
        isINDForestMoratoriumTile
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
