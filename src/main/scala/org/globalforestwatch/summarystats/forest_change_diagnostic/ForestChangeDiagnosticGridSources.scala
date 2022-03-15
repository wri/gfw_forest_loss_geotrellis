package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.Raster
import org.globalforestwatch.grids.{GridSources, GridTile}
import org.globalforestwatch.layers._

/**
  * @param gridTile top left corner, padded from east ex: "10N_010E"
  */
case class ForestChangeDiagnosticGridSources(gridTile: GridTile, kwargs: Map[String, Any])
  extends GridSources {

  val treeCoverLoss: TreeCoverLoss = TreeCoverLoss(gridTile, kwargs)
  val treeCoverDensity2000: TreeCoverDensityPercent2000 = TreeCoverDensityPercent2000(gridTile, kwargs)
  val isPrimaryForest: PrimaryForest = PrimaryForest(gridTile, kwargs)
  val isPeatlands: Peatlands = Peatlands(gridTile, kwargs)
  val isIntactForestLandscapes2000: IntactForestLandscapes2000 = IntactForestLandscapes2000(gridTile, kwargs)
  val protectedAreas: ProtectedAreas = ProtectedAreas(gridTile, kwargs)
  val seAsiaLandCover: SEAsiaLandCover = SEAsiaLandCover(gridTile, kwargs)
  val idnLandCover: IndonesiaLandCover = IndonesiaLandCover(gridTile, kwargs)
  val isSoyPlantedArea: SoyPlantedAreas = SoyPlantedAreas(gridTile, kwargs)
  val idnForestArea: IndonesiaForestArea = IndonesiaForestArea(gridTile, kwargs)
  val isIDNForestMoratorium: IndonesiaForestMoratorium = IndonesiaForestMoratorium(gridTile, kwargs)
  val prodesLossYear: ProdesLossYear = ProdesLossYear(gridTile, kwargs)
  val braBiomes: BrazilBiomes = BrazilBiomes(gridTile, kwargs)
  val isPlantation: PlantedForestsBool = PlantedForestsBool(gridTile, kwargs)
  val gfwProCoverage: GFWProCoverage = GFWProCoverage(gridTile, kwargs)


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
      val isIntactForestLandscapes2000Tile =
        isIntactForestLandscapes2000.fetchWindow(windowKey, windowLayout)
      val wdpaTile = protectedAreas.fetchWindow(windowKey, windowLayout)
      val seAsiaLandCoverTile = seAsiaLandCover.fetchWindow(windowKey, windowLayout)
      val idnLandCoverTile = idnLandCover.fetchWindow(windowKey, windowLayout)
      val isSoyPlantedAreasTile = isSoyPlantedArea.fetchWindow(windowKey, windowLayout)
      val idnForestAreaTile = idnForestArea.fetchWindow(windowKey, windowLayout)
      val isINDForestMoratoriumTile = isIDNForestMoratorium.fetchWindow(windowKey, windowLayout)
      val prodesLossYearTile = prodesLossYear.fetchWindow(windowKey, windowLayout)
      val braBiomesTile = braBiomes.fetchWindow(windowKey, windowLayout)
      val isPlantationTile = isPlantation.fetchWindow(windowKey, windowLayout)
      val gfwProCoverageTile = gfwProCoverage.fetchWindow(windowKey, windowLayout)


      val tile = ForestChangeDiagnosticTile(
        lossTile,
        tcd2000Tile,
        isPrimaryForestTile,
        isPeatlandsTile,
        isIntactForestLandscapes2000Tile,
        wdpaTile,
        seAsiaLandCoverTile,
        idnLandCoverTile,
        isSoyPlantedAreasTile,
        idnForestAreaTile,
        isINDForestMoratoriumTile,
        prodesLossYearTile,
        braBiomesTile,
        isPlantationTile,
        gfwProCoverageTile
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
                        gridTile: GridTile,
                        kwargs: Map[String, Any]
                      ): ForestChangeDiagnosticGridSources = {

    cache.getOrElseUpdate(
      gridTile.tileId,
      ForestChangeDiagnosticGridSources(gridTile, kwargs)
    )

  }

}
