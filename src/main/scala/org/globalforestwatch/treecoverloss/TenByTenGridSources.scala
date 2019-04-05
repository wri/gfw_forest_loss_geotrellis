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

  val treeCoverLoss = new TreeCoverLoss(grid)
  val treeCoverGain = new TreeCoverGain(grid)
  val treeCoverDensity2000 = new TreeCoverDensity2000(grid)
  val treeCoverDensity2010 = new TreeCoverDensity2010(grid)
  val carbon = new Carbon(grid)
  val biomassPerHectar = new BiomassPerHectar(grid)

  val mangroveBiomass = new MangroveBiomass(grid)
  val treeCoverLossDrivers = new TreeCoverLossDrivers(grid)
  val globalLandCover = new GlobalLandcover(grid)
  val primaryForest = new PrimaryForest(grid)
  val indonesiaPrimaryForest = new IndonesiaPrimaryForest(grid)
  val erosion = new Erosion(grid)
  val biodiversitySignificance = new BiodiversitySignificance(grid)
  val biodiversityIntactness = new BiodiversityIntactness(grid)
  val protectedAreas = new ProtectedAreas(grid)
  val plantations = new Plantations(grid)
  val riverBasins = new RiverBasins(grid)
  val ecozones = new Ecozones(grid)
  val urbanWatersheds = new UrbanWatersheds(grid)
  val mangroves1996 = new Mangroves1996(grid)
  val mangroves2016 = new Mangroves2016(grid)
  val waterStress = new WaterStress(grid)
  val intactForestLandscapes = new IntactForestLandscapes(grid)
  val endemicBirdAreas = new EndemicBirdAreas(grid)
  val tigerLandscapes = new TigerLandscapes(grid)
  val landmark = new Landmark(grid)
  val landRights = new LandRights(grid)
  val keyBiodiversityAreas = new KeyBiodiversityAreas(grid)
  val mining = new Mining(grid)
  val rspo = new RSPO(grid)
  val peatlands = new Peatlands(grid)
  val oilPalm = new OilPalm(grid)
  val indonesiaForestMoratorium = new IndonesiaForestMoratorium(grid)
  val indonesiaLandCover = new IndonesiaLandCover(grid)
  val indonesiaForestArea = new IndonesiaForestArea(grid)
  val mexicoProtectedAreas = new MexicoProtectedAreas(grid)
  val mexicoPaymentForEcosystemServices = new MexicoPaymentForEcosystemServices(grid)
  val mexicoForestZoning = new MexicoForestZoning(grid)
  val peruProductionForest = new PeruProductionForest(grid)
  val peruProtectedAreas = new PeruProtectedAreas(grid)
  val peruForestConcessions = new PeruForestConcessions(grid)
  val brazilBiomes = new BrazilBiomes(grid)
  val woodFiber = new WoodFiber(grid)
  val resourceRights = new ResourceRights(grid)
  val logging = new Logging(grid)

  def readWindow(window: Extent): Either[Throwable, Raster[TreeLossTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      lossTile <- Either.catchNonFatal(treeCoverLoss.fetchWindow(window)).right
      gainTile <- Either.catchNonFatal(treeCoverGain.fetchWindow(window)).right
      tcd2000Tile <- Either.catchNonFatal(treeCoverDensity2000.fetchWindow(window)).right
      tcd2010Tile <- Either.catchNonFatal(treeCoverDensity2010.fetchWindow(window)).right
      co2PixelTile <- Either.catchNonFatal(carbon.fetchWindow(window)).right
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

      val tile = TreeLossTile(
        lossTile,
        gainTile,
        tcd2000Tile,
        tcd2010Tile,
        co2PixelTile,
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
        loggingTile
      )

      Raster(tile, window)
    }
  }
}
