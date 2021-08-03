package org.globalforestwatch.summarystats.firealerts

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
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
  val treeCoverDensityThreshold2000 = TreeCoverDensityThreshold2000(gridTile)

  def readWindow(windowKey: SpatialKey, windowLayout: LayoutDefinition): Either[Throwable, Raster[FireAlertsTile]] = {
      // Failure for these will be converted to optional result and propagated with TreeLossTile
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
      val tcd2000Tile =
        treeCoverDensityThreshold2000.fetchWindow(windowKey, windowLayout)

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

  def getCachedSources(gridTile: GridTile): FireAlertsGridSources = {

    cache.getOrElseUpdate(gridTile.tileId, FireAlertsGridSources(gridTile))

  }

}
