package org.globalforestwatch.summarystats.gladalerts

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.Raster
import org.globalforestwatch.grids.{GridSources, GridTile}
import org.globalforestwatch.layers._

/**
  * @param gridTile top left corner, padded from east ex: "10N_010E"
  */
case class GladAlertsGridSources(gridTile: GridTile, kwargs: Map[String, Any]) extends GridSources {

  val gladAlerts: GladAlerts = GladAlerts(gridTile, kwargs)
  val biomassPerHectar: AbovegroundBiomass2000 = AbovegroundBiomass2000(gridTile, kwargs)
  val climateMask: ClimateMask = ClimateMask(gridTile, kwargs)
  val primaryForest: PrimaryForest = PrimaryForest(gridTile, kwargs)
  val protectedAreas: ProtectedAreas = ProtectedAreas(gridTile, kwargs)
  val aze: Aze = Aze(gridTile, kwargs)
  val keyBiodiversityAreas: KeyBiodiversityAreas = KeyBiodiversityAreas(gridTile, kwargs)
  val landmark: Landmark = Landmark(gridTile, kwargs)
  val plantedForests: PlantedForests = PlantedForests(gridTile, kwargs)
  val mining: Mining = Mining(gridTile, kwargs)
  val logging: Logging = Logging(gridTile, kwargs)
  val rspo: RSPO = RSPO(gridTile, kwargs)
  val woodFiber: WoodFiber = WoodFiber(gridTile, kwargs)
  val peatlands: Peatlands = Peatlands(gridTile, kwargs)
  val indonesiaForestMoratorium: IndonesiaForestMoratorium = IndonesiaForestMoratorium(gridTile, kwargs)
  val oilPalm: OilPalm = OilPalm(gridTile, kwargs)
  val indonesiaForestArea: IndonesiaForestArea = IndonesiaForestArea(gridTile, kwargs)
  val peruForestConcessions: PeruForestConcessions = PeruForestConcessions(gridTile, kwargs)
  val oilGas: OilGas = OilGas(gridTile, kwargs)
  val mangroves2020: Mangroves2020 = Mangroves2020(gridTile, kwargs)
  val intactForestLandscapes2016: IntactForestLandscapes2016 = IntactForestLandscapes2016(gridTile, kwargs)
  val braBiomes: BrazilBiomes = BrazilBiomes(gridTile, kwargs)
  val naturalForests: SBTNNaturalForests = SBTNNaturalForests(gridTile, kwargs)

  def readWindow(windowKey: SpatialKey, windowLayout: LayoutDefinition): Either[Throwable, Raster[GladAlertsTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      gladAlertsTile <- Either
        .catchNonFatal(gladAlerts.fetchWindow(windowKey, windowLayout))
        .right

    } yield {
      // Failure for these will be converted to optional result and propagated with TreeLossTile
      val biomassTile = biomassPerHectar.fetchWindow(windowKey, windowLayout)
      val climateMaskTile = climateMask.fetchWindow(windowKey, windowLayout)
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
      val indonesiaForestMoratoriumTile = indonesiaForestMoratorium.fetchWindow(windowKey, windowLayout)
      val oilPalmTile = oilPalm.fetchWindow(windowKey, windowLayout)
      val indonesiaForestAreaTile = indonesiaForestArea.fetchWindow(windowKey, windowLayout)
      val peruForestConcessionsTile = peruForestConcessions.fetchWindow(windowKey, windowLayout)
      val oilGasTile = oilGas.fetchWindow(windowKey, windowLayout)
      val mangroves2020Tile = mangroves2020.fetchWindow(windowKey, windowLayout)
      val intactForestLandscapes2016Tile = intactForestLandscapes2016.fetchWindow(windowKey, windowLayout)
      val braBiomesTile = braBiomes.fetchWindow(windowKey, windowLayout)
      val naturalForestsTile = naturalForests.fetchWindow(windowKey, windowLayout)

      val tile = GladAlertsTile(
        gladAlertsTile,
        biomassTile,
        climateMaskTile,
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
        mangroves2020Tile,
        intactForestLandscapes2016Tile,
        braBiomesTile,
        naturalForestsTile
      )

      Raster(tile, windowKey.extent(windowLayout))
    }
  }

}

object GladAlertsGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap.empty[String, GladAlertsGridSources]

  def getCachedSources(gridTile: GridTile, kwargs: Map[String, Any]): GladAlertsGridSources = {

    cache.getOrElseUpdate(gridTile.tileId, GladAlertsGridSources(gridTile, kwargs))

  }

}
