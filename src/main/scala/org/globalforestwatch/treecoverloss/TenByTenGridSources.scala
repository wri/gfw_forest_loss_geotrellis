package org.globalforestwatch.treecoverloss


import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.Raster
import geotrellis.vector.Extent
import cats.implicits._
import org.globalforestwatch.layers._

/**
  * @param grid top left corner, padded from east ex: "10N_010E"
  */
case class TenByTenGridSources(grid: String) extends LazyLogging {

  lazy val treeCoverLoss = new TreeCoverLoss(grid)
  lazy val treeCoverGain = new TreeCoverGain(grid)
  lazy val treeCoverDensity2000 = new TreeCoverDensity2000(grid)
  lazy val treeCoverDensity2010 = new TreeCoverDensity2010(grid)
  //lazy val carbon = new Carbon(grid)
  lazy val biomassPerHectar = new BiomassPerHectar(grid)

  lazy val mangroveBiomass = new MangroveBiomass(grid)
  lazy val treeCoverLossDrivers = new TreeCoverLossDrivers(grid)
  lazy val globalLandCover = new GlobalLandcover(grid)
  lazy val primaryForest = new PrimaryForest(grid)
  lazy val indonesiaPrimaryForest = new IndonesiaPrimaryForest(grid)
  lazy val erosion = new Erosion(grid)
  // lazy val biodiversitySignificance = new BiodiversitySignificance(grid)
  // lazy val biodiversityIntactness = new BiodiversityIntactness(grid)
  lazy val protectedAreas = new ProtectedAreas(grid)
  lazy val plantations = new Plantations(grid)
  lazy val riverBasins = new RiverBasins(grid)
  lazy val ecozones = new Ecozones(grid)
  lazy val urbanWatersheds = new UrbanWatersheds(grid)
  lazy val mangroves1996 = new Mangroves1996(grid)
  lazy val mangroves2016 = new Mangroves2016(grid)
  lazy val waterStress = new WaterStress(grid)
  lazy val intactForestLandscapes = new IntactForestLandscapes(grid)
  lazy val endemicBirdAreas = new EndemicBirdAreas(grid)
  lazy val tigerLandscapes = new TigerLandscapes(grid)
  lazy val landmark = new Landmark(grid)
  lazy val landRights = new LandRights(grid)
  lazy val keyBiodiversityAreas = new KeyBiodiversityAreas(grid)
  lazy val mining = new Mining(grid)
  lazy val rspo = new RSPO(grid)
  lazy val peatlands = new Peatlands(grid)
  lazy val oilPalm = new OilPalm(grid)
  lazy val indonesiaForestMoratorium = new IndonesiaForestMoratorium(grid)
  lazy val indonesiaLandCover = new IndonesiaLandCover(grid)
  lazy val indonesiaForestArea = new IndonesiaForestArea(grid)
  lazy val mexicoProtectedAreas = new MexicoProtectedAreas(grid)
  lazy val mexicoPaymentForEcosystemServices = new MexicoPaymentForEcosystemServices(grid)
  lazy val mexicoForestZoning = new MexicoForestZoning(grid)
  lazy val peruProductionForest = new PeruProductionForest(grid)
  lazy val peruProtectedAreas = new PeruProtectedAreas(grid)
  lazy val peruForestConcessions = new PeruForestConcessions(grid)
  lazy val brazilBiomes = new BrazilBiomes(grid)
  lazy val woodFiber = new WoodFiber(grid)
  lazy val resourceRights = new ResourceRights(grid)
  lazy val logging = new Logging(grid)
  lazy val oilGas = new OilGas(grid)

  def readWindow(window: Extent): Either[Throwable, Raster[TreeLossTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      lossTile <- Either.catchNonFatal(treeCoverLoss.fetchWindow(window)).right
      gainTile <- Either.catchNonFatal(treeCoverGain.fetchWindow(window)).right
      tcd2000Tile <- Either.catchNonFatal(treeCoverDensity2000.fetchWindow(window)).right
      tcd2010Tile <- Either.catchNonFatal(treeCoverDensity2010.fetchWindow(window)).right
      // co2PixelTile <- Either.catchNonFatal(carbon.fetchWindow(window)).right
      biomassTile <- Either.catchNonFatal(biomassPerHectar.fetchWindow(window)).right

    } yield {
      // Failure for these will be converted to optional result and propagated with TreeLossTile
      val mangroveBiomassTile = mangroveBiomass.fetchWindow(window)
      val driversTile = treeCoverLossDrivers.fetchWindow(window)
      val globalLandCoverTile = globalLandCover.fetchWindow(window)
      val primaryForestTile = primaryForest.fetchWindow(window)
      val idnPrimaryForestTile = indonesiaPrimaryForest.fetchWindow(window)
      val erosionTile = erosion.fetchWindow(window)
      // val biodiversitySignificanceTile = biodiversitySignificance.fetchWindow(window)
      // val biodiversityIntactnessTile = biodiversityIntactness.fetchWindow(window)
      val wdpaTile = protectedAreas.fetchWindow(window)
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
        // co2PixelTile,
        biomassTile,
        mangroveBiomassTile,
        driversTile,
        globalLandCoverTile,
        primaryForestTile,
        idnPrimaryForestTile,
        erosionTile,
        // biodiversitySignificanceTile,
        // biodiversityIntactnessTile,
        wdpaTile,
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
