package org.globalforestwatch.summarystats.integrated_alerts

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.Raster
import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridId, GridSources, GridTile}
import org.globalforestwatch.layers._

/**
  * @param gridTile top left corner, padded from east ex: "10N_010E"
  */
case class IntegratedAlertsGridSources(gridTile: GridTile) extends GridSources {

  val gladAlertsL = GladAlerts(gridTile)
  val gladAlertsS2 = GladAlertsS2(gridTile)
  val raddAlerts = RaddAlerts(gridTile)
  val biomassPerHectar = BiomassPerHectar(gridTile)
  val climateMask = ClimateMask(gridTile)
  val primaryForest = PrimaryForest(gridTile)
  val protectedAreas = ProtectedAreas(gridTile)
  val aze = Aze(gridTile)
  val keyBiodiversityAreas = KeyBiodiversityAreas(gridTile)
  val landmark = Landmark(gridTile)
  val plantations = Plantations(gridTile)
  val mining = Mining(gridTile)
  val logging = Logging(gridTile)
  val rspo = RSPO(gridTile)
  val woodFiber = WoodFiber(gridTile)
  val peatlands = Peatlands(gridTile)
  val indonesiaForestMoratorium = IndonesiaForestMoratorium(gridTile)
  val oilPalm = OilPalm(gridTile)
  val indonesiaForestArea = IndonesiaForestArea(gridTile)
  val peruForestConcessions = PeruForestConcessions(gridTile)
  val oilGas = OilGas(gridTile)
  val mangroves2016 = Mangroves2016(gridTile)
  val intactForestLandscapes2016 = IntactForestLandscapes2016(gridTile)
  val braBiomes = BrazilBiomes(gridTile)

  def readWindow(windowKey: SpatialKey, windowLayout: LayoutDefinition): Either[Throwable, Raster[IntegratedAlertsTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      gladAlertsLTile <- Either
        .catchNonFatal(gladAlertsL.fetchWindow(windowKey, windowLayout))
        .right

    } yield {
      val gladAlertsS2Tile = gladAlertsS2.fetchWindow(windowKey, windowLayout)
      val raddAlertsTile = raddAlerts.fetchWindow(windowKey, windowLayout)
      val biomassTile = biomassPerHectar.fetchWindow(windowKey, windowLayout)
      val climateMaskTile = climateMask.fetchWindow(windowKey, windowLayout)
      val primaryForestTile = primaryForest.fetchWindow(windowKey, windowLayout)
      val protectedAreasTile = protectedAreas.fetchWindow(windowKey, windowLayout)
      val azeTile = aze.fetchWindow(windowKey, windowLayout)
      val keyBiodiversityAreasTile = keyBiodiversityAreas.fetchWindow(windowKey, windowLayout)
      val landmarkTile = landmark.fetchWindow(windowKey, windowLayout)
      val plantationsTile = plantations.fetchWindow(windowKey, windowLayout)
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

      val tile = IntegratedAlertsTile(
        gladAlertsLTile,
        gladAlertsS2Tile,
        raddAlertsTile,
        biomassTile,
        climateMaskTile,
        primaryForestTile,
        protectedAreasTile,
        azeTile,
        keyBiodiversityAreasTile,
        landmarkTile,
        plantationsTile,
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
        braBiomesTile
      )

      Raster(tile, windowKey.extent(windowLayout))
    }
  }

}

object IntegratedAlertsGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap.empty[String, IntegratedAlertsGridSources]

  def getCachedSources(gridTile: GridTile): IntegratedAlertsGridSources = {

    cache.getOrElseUpdate(gridTile.tileId, IntegratedAlertsGridSources(gridTile))

  }

}