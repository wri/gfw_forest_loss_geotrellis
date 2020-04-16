package org.globalforestwatch.summarystats.firealerts

import cats.implicits._
import geotrellis.raster.Raster
import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridSources, GridTile}
import org.globalforestwatch.layers._

/**
  * @param gridTile top left corner, padded from east ex: "10N_010E"
  */
case class FireAlertsGridSources(gridTile: GridTile) extends GridSources {
  val primaryForest = PrimaryForest(gridTile)
  val keyBiodiversityAreas = KeyBiodiversityAreas(gridTile) // not found
  val aze = Aze(gridTile)
  val landmark = Landmark(gridTile) // not found
  val plantations = Plantations(gridTile)
  val mining = Mining(gridTile)
  val oilPalm = OilPalm(gridTile)
  val peatlands = Peatlands(gridTile)
  val rspo = RSPO(gridTile)
  val woodFiber = WoodFiber(gridTile)
  val indonesiaForestMoratorium = IndonesiaForestMoratorium(gridTile)
  val indonesiaForestArea = IndonesiaForestArea(gridTile)
  val oilGas = OilGas(gridTile)
  val logging = Logging(gridTile)
  val braBiomes = BrazilBiomes(gridTile)
  val mangroves2016 = Mangroves2016(gridTile)
  val intactForestLandscapes2016 = IntactForestLandscapes2016(gridTile)
  val peruForestConcessions = PeruForestConcessions(gridTile)
  val protectedAreas = ProtectedAreas(gridTile)

  def readWindow(window: Extent): Either[Throwable, Raster[FireAlertsTile]] = {
      // Failure for these will be converted to optional result and propagated with TreeLossTile
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

      val tile = FireAlertsTile(
        gridTile,
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

      Right(Raster(tile, window))
    }
}

object FireAlertsGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap.empty[String, FireAlertsGridSources]

  def getCachedSources(gridTile: GridTile): FireAlertsGridSources = {

    cache.getOrElseUpdate(gridTile.tileId, FireAlertsGridSources(gridTile))

  }

}
