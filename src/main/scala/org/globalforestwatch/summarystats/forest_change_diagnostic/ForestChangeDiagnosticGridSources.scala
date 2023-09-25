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
  val seAsiaLandCover: SEAsiaLandCover = SEAsiaLandCover(gridTile, kwargs)
  val idnLandCover: IndonesiaLandCover = IndonesiaLandCover(gridTile, kwargs)
  val isSoyPlantedArea: SoyPlantedAreas = SoyPlantedAreas(gridTile, kwargs)
  val idnForestArea: IndonesiaForestArea = IndonesiaForestArea(gridTile, kwargs)
  val isIDNForestMoratorium: IndonesiaForestMoratorium = IndonesiaForestMoratorium(gridTile, kwargs)
  val prodesLossYear: ProdesLossYear = ProdesLossYear(gridTile, kwargs)
  val braBiomes: BrazilBiomes = BrazilBiomes(gridTile, kwargs)
  val isPlantation: PlantedForestsBool = PlantedForestsBool(gridTile, kwargs)
  val gfwProCoverage: GFWProCoverage = GFWProCoverage(gridTile, kwargs)
  val argOTBN: ArgOTBN = ArgOTBN(gridTile, kwargs)
  val protectedAreasByCategory: DetailedProtectedAreas = DetailedProtectedAreas(gridTile, kwargs)
  val landmark: Landmark = Landmark(gridTile, kwargs)


  def readWindow(
                  windowKey: SpatialKey,
                  windowLayout: LayoutDefinition
                ): Either[Throwable, Raster[ForestChangeDiagnosticTile]] = {

    val tile = ForestChangeDiagnosticTile(
      windowKey, windowLayout, this
    )
    Right(Raster(tile, windowKey.extent(windowLayout)))
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
