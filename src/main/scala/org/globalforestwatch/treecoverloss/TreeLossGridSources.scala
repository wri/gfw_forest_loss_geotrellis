package org.globalforestwatch.treecoverloss


import geotrellis.raster.Raster
import geotrellis.vector.Extent
import cats.implicits._
import org.globalforestwatch.grids.GridSources
import org.globalforestwatch.layers._

/**
  * @param grid top left corner, padded from east ex: "10N_010E"
  */
case class TreeLossGridSources(grid: String) extends GridSources {

  lazy val treeCoverLoss = TreeCoverLoss(grid)
  lazy val treeCoverGain = TreeCoverGain(grid)
  lazy val treeCoverDensity2000 = TreeCoverDensity2000(grid)
  lazy val treeCoverDensity2010 = TreeCoverDensity2010(grid)
  lazy val biomassPerHectar = BiomassPerHectar(grid)

  lazy val mangroveBiomass = MangroveBiomass(grid)
  lazy val treeCoverLossDrivers = TreeCoverLossDrivers(grid)
  lazy val globalLandCover = GlobalLandcover(grid)
  lazy val primaryForest = PrimaryForest(grid)
  lazy val indonesiaPrimaryForest = IndonesiaPrimaryForest(grid)
  lazy val erosion = Erosion(grid)
  lazy val biodiversitySignificance = BiodiversitySignificance(grid)
  lazy val biodiversityIntactness = BiodiversityIntactness(grid)
  lazy val protectedAreas = ProtectedAreas(grid)
  lazy val aze = Aze(grid)
  lazy val plantations = Plantations(grid)
  lazy val riverBasins = RiverBasins(grid)
  lazy val ecozones = Ecozones(grid)
  lazy val urbanWatersheds = UrbanWatersheds(grid)
  lazy val mangroves1996 = Mangroves1996(grid)
  lazy val mangroves2016 = Mangroves2016(grid)
  lazy val waterStress = WaterStress(grid)
  lazy val intactForestLandscapes = IntactForestLandscapes(grid)
  lazy val endemicBirdAreas = EndemicBirdAreas(grid)
  lazy val tigerLandscapes = TigerLandscapes(grid)
  lazy val landmark = Landmark(grid)
  lazy val landRights = LandRights(grid)
  lazy val keyBiodiversityAreas = KeyBiodiversityAreas(grid)
  lazy val mining = Mining(grid)
  lazy val rspo = RSPO(grid)
  lazy val peatlands = Peatlands(grid)
  lazy val oilPalm = OilPalm(grid)
  lazy val indonesiaForestMoratorium = IndonesiaForestMoratorium(grid)
  lazy val indonesiaLandCover = IndonesiaLandCover(grid)
  lazy val indonesiaForestArea = IndonesiaForestArea(grid)
  lazy val mexicoProtectedAreas = MexicoProtectedAreas(grid)
  lazy val mexicoPaymentForEcosystemServices = MexicoPaymentForEcosystemServices(grid)
  lazy val mexicoForestZoning = MexicoForestZoning(grid)
  lazy val peruProductionForest = PeruProductionForest(grid)
  lazy val peruProtectedAreas = PeruProtectedAreas(grid)
  lazy val peruForestConcessions = PeruForestConcessions(grid)
  lazy val brazilBiomes = BrazilBiomes(grid)
  lazy val woodFiber = WoodFiber(grid)
  lazy val resourceRights = ResourceRights(grid)
  lazy val logging = Logging(grid)
  lazy val oilGas = OilGas(grid)

  def readWindow(window: Extent): Either[Throwable, Raster[TreeLossTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      lossTile <- Either.catchNonFatal(treeCoverLoss.fetchWindow(window)).right
      gainTile <- Either.catchNonFatal(treeCoverGain.fetchWindow(window)).right
      tcd2000Tile <- Either.catchNonFatal(treeCoverDensity2000.fetchWindow(window)).right
      tcd2010Tile <- Either.catchNonFatal(treeCoverDensity2010.fetchWindow(window)).right
      biomassTile <- Either.catchNonFatal(biomassPerHectar.fetchWindow(window)).right

    } yield {
      // Failure for these will be converted to optional result and propagated with TreeLossTile
      val mangroveBiomassTile = mangroveBiomass.fetchWindow(window)
      val driversTile = treeCoverLossDrivers.fetchWindow(window)
      val globalLandCoverTile = globalLandCover.fetchWindow(window)
      val primaryForestTile = primaryForest.fetchWindow(window)
      val idnPrimaryForestTile = indonesiaPrimaryForest.fetchWindow(window)
      val erosionTile = erosion.fetchWindow(window)
      val biodiversitySignificanceTile = biodiversitySignificance.fetchWindow(window)
      val biodiversityIntactnessTile = biodiversityIntactness.fetchWindow(window)
      val wdpaTile = protectedAreas.fetchWindow(window)
      val azeTile = aze.fetchWindow(window)
      val plantationsTile = plantations.fetchWindow(window)
      val riverBasinsTile = riverBasins.fetchWindow(window)
      val ecozonesTile = ecozones.fetchWindow(window)
      val urbanWatershedsTile = urbanWatersheds.fetchWindow(window)
      val mangroves1996Tile = mangroves1996.fetchWindow(window)
      val mangroves2016Tile = mangroves2016.fetchWindow(window)
      val waterStressTile = waterStress.fetchWindow(window)
      val intactForestLandscapesTile = intactForestLandscapes.fetchWindow(window)
      val endemicBirdAreasTile = endemicBirdAreas.fetchWindow(window)
      val tigerLandscapesTile = tigerLandscapes.fetchWindow(window)
      val landmarkTile = landmark.fetchWindow(window)
      val landRightsTile = landRights.fetchWindow(window)
      val keyBiodiversityAreasTile = keyBiodiversityAreas.fetchWindow(window)
      val miningTile = mining.fetchWindow(window)
      val rspoTile = rspo.fetchWindow(window)
      val peatlandsTile = peatlands.fetchWindow(window)
      val oilPalmTile = oilPalm.fetchWindow(window)
      val idnForestMoratoriumTile = indonesiaForestMoratorium.fetchWindow(window)
      val idnLandCoverTile = indonesiaLandCover.fetchWindow(window)
      val idnForestAreaTile = indonesiaForestArea.fetchWindow(window)
      val mexProtectedAreasTile = mexicoProtectedAreas.fetchWindow(window)
      val mexPaymentForEcosystemServicesTile = mexicoPaymentForEcosystemServices.fetchWindow(window)
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
