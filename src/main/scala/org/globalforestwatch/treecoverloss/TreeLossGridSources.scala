package org.globalforestwatch.treecoverloss

import geotrellis.raster.Raster
import geotrellis.vector.Extent
import cats.implicits._
import org.globalforestwatch.grids.GridSources
import org.globalforestwatch.layers._

/**
  * @param gridId top left corner, padded from east ex: "10N_010E"
  */
case class TreeLossGridSources(gridId: String) extends GridSources {

  lazy val treeCoverLoss = TreeCoverLoss(gridId)
  lazy val treeCoverGain = TreeCoverGain(gridId)
  lazy val treeCoverDensity2000 = TreeCoverDensity2000(gridId)
  lazy val treeCoverDensity2010 = TreeCoverDensity2010(gridId)
  lazy val biomassPerHectar = BiomassPerHectar(gridId)

  lazy val mangroveBiomass = MangroveBiomass(gridId)
  lazy val treeCoverLossDrivers = TreeCoverLossDrivers(gridId)
  lazy val globalLandCover = GlobalLandcover(gridId)
  lazy val primaryForest = PrimaryForest(gridId)
  lazy val indonesiaPrimaryForest = IndonesiaPrimaryForest(gridId)
  lazy val erosion = Erosion(gridId)
  lazy val biodiversitySignificance = BiodiversitySignificance(gridId)
  lazy val biodiversityIntactness = BiodiversityIntactness(gridId)
  lazy val protectedAreas = ProtectedAreas(gridId)
  lazy val aze = Aze(gridId)
  lazy val plantations = Plantations(gridId)
  lazy val riverBasins = RiverBasins(gridId)
  lazy val ecozones = Ecozones(gridId)
  lazy val urbanWatersheds = UrbanWatersheds(gridId)
  lazy val mangroves1996 = Mangroves1996(gridId)
  lazy val mangroves2016 = Mangroves2016(gridId)
  lazy val waterStress = WaterStress(gridId)
  lazy val intactForestLandscapes = IntactForestLandscapes(gridId)
  lazy val endemicBirdAreas = EndemicBirdAreas(gridId)
  lazy val tigerLandscapes = TigerLandscapes(gridId)
  lazy val landmark = Landmark(gridId)
  lazy val landRights = LandRights(gridId)
  lazy val keyBiodiversityAreas = KeyBiodiversityAreas(gridId)
  lazy val mining = Mining(gridId)
  lazy val rspo = RSPO(gridId)
  lazy val peatlands = Peatlands(gridId)
  lazy val oilPalm = OilPalm(gridId)
  lazy val indonesiaForestMoratorium = IndonesiaForestMoratorium(gridId)
  lazy val indonesiaLandCover = IndonesiaLandCover(gridId)
  lazy val indonesiaForestArea = IndonesiaForestArea(gridId)
  lazy val mexicoProtectedAreas = MexicoProtectedAreas(gridId)
  lazy val mexicoPaymentForEcosystemServices =
    MexicoPaymentForEcosystemServices(gridId)
  lazy val mexicoForestZoning = MexicoForestZoning(gridId)
  lazy val peruProductionForest = PeruProductionForest(gridId)
  lazy val peruProtectedAreas = PeruProtectedAreas(gridId)
  lazy val peruForestConcessions = PeruForestConcessions(gridId)
  lazy val brazilBiomes = BrazilBiomes(gridId)
  lazy val woodFiber = WoodFiber(gridId)
  lazy val resourceRights = ResourceRights(gridId)
  lazy val logging = Logging(gridId)
  lazy val oilGas = OilGas(gridId)

  def readWindow(window: Extent): Either[Throwable, Raster[TreeLossTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      lossTile <- Either.catchNonFatal(treeCoverLoss.fetchWindow(window)).right
      gainTile <- Either.catchNonFatal(treeCoverGain.fetchWindow(window)).right
      tcd2000Tile <- Either
        .catchNonFatal(treeCoverDensity2000.fetchWindow(window))
        .right
      tcd2010Tile <- Either
        .catchNonFatal(treeCoverDensity2010.fetchWindow(window))
        .right

    } yield {
      // Failure for these will be converted to optional result and propagated with TreeLossTile
      val biomassTile = biomassPerHectar.fetchWindow(window)
      val mangroveBiomassTile = mangroveBiomass.fetchWindow(window)
      val driversTile = treeCoverLossDrivers.fetchWindow(window)
      val globalLandCoverTile = globalLandCover.fetchWindow(window)
      val primaryForestTile = primaryForest.fetchWindow(window)
      val idnPrimaryForestTile = indonesiaPrimaryForest.fetchWindow(window)
      val erosionTile = erosion.fetchWindow(window)
      val biodiversitySignificanceTile =
        biodiversitySignificance.fetchWindow(window)
      val biodiversityIntactnessTile =
        biodiversityIntactness.fetchWindow(window)
      val wdpaTile = protectedAreas.fetchWindow(window)
      val azeTile = aze.fetchWindow(window)
      val plantationsTile = plantations.fetchWindow(window)
      val riverBasinsTile = riverBasins.fetchWindow(window)
      val ecozonesTile = ecozones.fetchWindow(window)
      val urbanWatershedsTile = urbanWatersheds.fetchWindow(window)
      val mangroves1996Tile = mangroves1996.fetchWindow(window)
      val mangroves2016Tile = mangroves2016.fetchWindow(window)
      val waterStressTile = waterStress.fetchWindow(window)
      val intactForestLandscapesTile =
        intactForestLandscapes.fetchWindow(window)
      val endemicBirdAreasTile = endemicBirdAreas.fetchWindow(window)
      val tigerLandscapesTile = tigerLandscapes.fetchWindow(window)
      val landmarkTile = landmark.fetchWindow(window)
      val landRightsTile = landRights.fetchWindow(window)
      val keyBiodiversityAreasTile = keyBiodiversityAreas.fetchWindow(window)
      val miningTile = mining.fetchWindow(window)
      val rspoTile = rspo.fetchWindow(window)
      val peatlandsTile = peatlands.fetchWindow(window)
      val oilPalmTile = oilPalm.fetchWindow(window)
      val idnForestMoratoriumTile =
        indonesiaForestMoratorium.fetchWindow(window)
      val idnLandCoverTile = indonesiaLandCover.fetchWindow(window)
      val idnForestAreaTile = indonesiaForestArea.fetchWindow(window)
      val mexProtectedAreasTile = mexicoProtectedAreas.fetchWindow(window)
      val mexPaymentForEcosystemServicesTile =
        mexicoPaymentForEcosystemServices.fetchWindow(window)
      val mexForestZoningTile = mexicoForestZoning.fetchWindow(window)
      val perProductionForestTile = peruProductionForest.fetchWindow(window)
      val perProtectedAreasTile = peruProtectedAreas.fetchWindow(window)
      val perForestConcessionsTile = peruForestConcessions.fetchWindow(window)
      val braBiomesTile = brazilBiomes.fetchWindow(window)
      val woodFiberTile = woodFiber.fetchWindow(window)
      val resourceRightsTile = resourceRights.fetchWindow(window)
      val loggingTile = logging.fetchWindow(window)
      val oilGasTile = oilGas.fetchWindow(window)

      val tile = TreeLossTile(
        lossTile,
        gainTile,
        tcd2000Tile,
        tcd2010Tile,
        biomassTile,
        mangroveBiomassTile,
        driversTile,
        globalLandCoverTile,
        primaryForestTile,
        idnPrimaryForestTile,
        erosionTile,
        biodiversitySignificanceTile,
        biodiversityIntactnessTile,
        wdpaTile,
        azeTile,
        plantationsTile,
        riverBasinsTile,
        ecozonesTile,
        urbanWatershedsTile,
        mangroves1996Tile,
        mangroves2016Tile,
        waterStressTile,
        intactForestLandscapesTile,
        endemicBirdAreasTile,
        tigerLandscapesTile,
        landmarkTile,
        landRightsTile,
        keyBiodiversityAreasTile,
        miningTile,
        rspoTile,
        peatlandsTile,
        oilPalmTile,
        idnForestMoratoriumTile,
        idnLandCoverTile,
        mexProtectedAreasTile,
        mexPaymentForEcosystemServicesTile,
        mexForestZoningTile,
        perProductionForestTile,
        perProtectedAreasTile,
        perForestConcessionsTile,
        braBiomesTile,
        woodFiberTile,
        resourceRightsTile,
        loggingTile,
        oilGasTile
      )

      Raster(tile, window)
    }
  }
}

object TreeLossGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap.empty[String, TreeLossGridSources]

  def getCachedSources(grid: String): TreeLossGridSources = {

    cache.getOrElseUpdate(grid, TreeLossGridSources(grid))

  }

}
