package org.globalforestwatch.summarystats.firealerts

import cats.implicits._
import geotrellis.raster.Raster
import geotrellis.vector.Extent
import org.globalforestwatch.grids.GridSources
import org.globalforestwatch.layers._

/**
  * @param gridId top left corner, padded from east ex: "10N_010E"
  */
case class FireAlertsGridSources(gridId: String) extends GridSources {
  val primaryForest = PrimaryForest(gridId)
  val keyBiodiversityAreas = KeyBiodiversityAreas(gridId) // not found
  val aze = Aze(gridId)
  val landmark = Landmark(gridId) // not found
  val plantations = Plantations(gridId)
  val mining = Mining(gridId)
  val oilPalm = OilPalm(gridId)
  val peatlands = Peatlands(gridId)
  val rspo = RSPO(gridId)
  val woodFiber = WoodFiber(gridId)
  val indonesiaForestMoratorium = IndonesiaForestMoratorium(gridId)
  val indonesiaForestArea = IndonesiaForestArea(gridId)
  val oilGas = OilGas(gridId)
  val logging = Logging(gridId)
  val braBiomes = BrazilBiomes(gridId)
  val mangroves2016 = Mangroves2016(gridId)
  val intactForestLandscapes2016 = IntactForestLandscapes2016(gridId)
  val peruForestConcessions = PeruForestConcessions(gridId)
  val protectedAreas = ProtectedAreas(gridId)

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

  def getCachedSources(gridId: String): FireAlertsGridSources = {

    cache.getOrElseUpdate(gridId, FireAlertsGridSources(gridId))

  }

}
