package org.globalforestwatch.summarystats.firealerts

import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.Raster
import org.globalforestwatch.grids.{GridSources, GridTile}
import org.globalforestwatch.layers._

/**
  * @param gridTile top left corner, padded from east ex: "10N_010E"
  */
case class FireAlertsGridSources(gridTile: GridTile, kwargs: Map[String, Any]) extends GridSources {
  val primaryForest: PrimaryForest = PrimaryForest(gridTile, kwargs)
  val keyBiodiversityAreas: KeyBiodiversityAreas = KeyBiodiversityAreas(gridTile, kwargs) // not found
  val aze: Aze = Aze(gridTile, kwargs)
  val landmark: Landmark = Landmark(gridTile, kwargs) // not found
  val plantedForests: PlantedForests = PlantedForests(gridTile, kwargs)
  val mining: Mining = Mining(gridTile, kwargs)
  val oilPalm: OilPalm = OilPalm(gridTile, kwargs)
  val peatlands: Peatlands = Peatlands(gridTile, kwargs)
  val rspo: RSPO = RSPO(gridTile, kwargs)
  val woodFiber: WoodFiber = WoodFiber(gridTile, kwargs)
  val indonesiaForestMoratorium: IndonesiaForestMoratorium = IndonesiaForestMoratorium(gridTile, kwargs)
  val indonesiaForestArea: IndonesiaForestArea = IndonesiaForestArea(gridTile, kwargs)
  val oilGas: OilGas = OilGas(gridTile, kwargs)
  val logging: Logging = Logging(gridTile, kwargs)
  val braBiomes: BrazilBiomes = BrazilBiomes(gridTile, kwargs)
  val mangroves2016: Mangroves2016 = Mangroves2016(gridTile, kwargs)
  val intactForestLandscapes2016: IntactForestLandscapes2016 = IntactForestLandscapes2016(gridTile, kwargs)
  val peruForestConcessions: PeruForestConcessions = PeruForestConcessions(gridTile, kwargs)
  val protectedAreas: ProtectedAreas = ProtectedAreas(gridTile, kwargs)
  val treeCoverDensityThreshold2000: TreeCoverDensityThreshold2000 = TreeCoverDensityThreshold2000(gridTile, kwargs)

  def readWindow(windowKey: SpatialKey, windowLayout: LayoutDefinition): Either[Throwable, Raster[FireAlertsTile]] = {
    // Failure for these will be converted to optional result and propagated with TreeLossTile
    val primaryForestTile = primaryForest.fetchWindow(windowKey, windowLayout)
    val protectedAreasTile = protectedAreas.fetchWindow(windowKey, windowLayout)
    val azeTile = aze.fetchWindow(windowKey, windowLayout)
    val keyBiodiversityAreasTile = keyBiodiversityAreas.fetchWindow(windowKey, windowLayout)
    val landmarkTile = landmark.fetchWindow(windowKey, windowLayout)
    val plantedForestsTile = plantedForests.fetchWindow(windowKey, windowLayout)
    val miningTile = mining.fetchWindow(windowKey, windowLayout)
      val loggingTile = logging.fetchWindow(windowKey, windowLayout)
      val rspoTile = rspo.fetchWindow(windowKey, windowLayout)
      val woodFiberTile = woodFiber.fetchWindow(windowKey, windowLayout)
      val peatlandsTile = peatlands.fetchWindow(windowKey, windowLayout)
      val indonesiaForestMoratoriumTile =
        indonesiaForestMoratorium.fetchWindow(windowKey, windowLayout)
      val oilPalmTile = oilPalm.fetchWindow(windowKey, windowLayout)
      val indonesiaForestAreaTile = indonesiaForestArea.fetchWindow(windowKey, windowLayout)
      val peruForestConcessionsTile = peruForestConcessions.fetchWindow(windowKey, windowLayout)
      val oilGasTile = oilGas.fetchWindow(windowKey, windowLayout)
      val mangroves2016Tile = mangroves2016.fetchWindow(windowKey, windowLayout)
      val intactForestLandscapes2016Tile =
        intactForestLandscapes2016.fetchWindow(windowKey, windowLayout)
      val braBiomesTile =
        braBiomes.fetchWindow(windowKey, windowLayout)
      val tcd2000Tile =
        treeCoverDensityThreshold2000.fetchWindow(windowKey, windowLayout)

      val tile = FireAlertsTile(
        gridTile,
        primaryForestTile,
        protectedAreasTile,
        azeTile,
        keyBiodiversityAreasTile,
        landmarkTile,
        plantedForestsTile,
        miningTile,
        loggingTile,
        rspoTile,
        woodFiberTile,
        peatlandsTile,
        indonesiaForestMoratoriumTile,
        oilPalmTile,
        indonesiaForestAreaTile,
        peruForestConcessionsTile,
        oilGasTile,
        mangroves2016Tile,
        intactForestLandscapes2016Tile,
        braBiomesTile,
        tcd2000Tile,
      )

      Right(Raster(tile, windowKey.extent(windowLayout)))
    }
}

object FireAlertsGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap.empty[String, FireAlertsGridSources]

  def getCachedSources(gridTile: GridTile, kwargs: Map[String, Any]): FireAlertsGridSources = {

    cache.getOrElseUpdate(gridTile.tileId, FireAlertsGridSources(gridTile, kwargs))

  }

}
