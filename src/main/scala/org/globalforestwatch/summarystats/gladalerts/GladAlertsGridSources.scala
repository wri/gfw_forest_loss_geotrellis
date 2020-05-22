package org.globalforestwatch.summarystats.gladalerts

import cats.implicits._
import geotrellis.raster.Raster
import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridId, GridSources, GridTile}
import org.globalforestwatch.layers._

/**
  * @param gridTile top left corner, padded from east ex: "10N_010E"
  */
case class GladAlertsGridSources(gridTile: GridTile) extends GridSources {

  val gladAlerts = GladAlerts(gridTile)
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

  def readWindow(window: Extent): Either[Throwable, Raster[GladAlertsTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      gladAlertsTile <- Either
        .catchNonFatal(gladAlerts.fetchWindow(window))
        .right

    } yield {
      // Failure for these will be converted to optional result and propagated with TreeLossTile
      val biomassTile = biomassPerHectar.fetchWindow(window)
      val climateMaskTile = climateMask.fetchWindow(window)
      val primaryForestTile = primaryForest.fetchWindow(window)
      val protectedAreasTile = protectedAreas.fetchWindow(window)
      val azeTile = aze.fetchWindow(window)
      val keyBiodiversityAreasTile = keyBiodiversityAreas.fetchWindow(window)
      val landmarkTile = landmark.fetchWindow(window)
      val plantationsTile = plantations.fetchWindow(window)
      val miningTile = mining.fetchWindow(window)
      val loggingTile = logging.fetchWindow(window)
      val rspoTile = rspo.fetchWindow(window)
      val woodFiberTile = woodFiber.fetchWindow(window)
      val peatlandsTile = peatlands.fetchWindow(window)
      val indonesiaForestMoratoriumTile =
        indonesiaForestMoratorium.fetchWindow(window)
      val oilPalmTile = oilPalm.fetchWindow(window)
      val indonesiaForestAreaTile = indonesiaForestArea.fetchWindow(window)
      val peruForestConcessionsTile = peruForestConcessions.fetchWindow(window)
      val oilGasTile = oilGas.fetchWindow(window)
      val mangroves2016Tile = mangroves2016.fetchWindow(window)
      val intactForestLandscapes2016Tile =
        intactForestLandscapes2016.fetchWindow(window)
      val braBiomesTile =
        braBiomes.fetchWindow(window)

      val tile = GladAlertsTile(
        gladAlertsTile,
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

      Raster(tile, window)
    }
  }

}

object GladAlertsGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap.empty[String, GladAlertsGridSources]

  def getCachedSources(gridTile: GridTile): GladAlertsGridSources = {

    cache.getOrElseUpdate(gridTile.tileId, GladAlertsGridSources(gridTile))

  }

}
