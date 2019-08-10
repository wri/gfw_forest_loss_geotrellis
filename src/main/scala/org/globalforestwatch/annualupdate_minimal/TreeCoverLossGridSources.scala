package org.globalforestwatch.annualupdate_minimal

import geotrellis.raster.Raster
import geotrellis.vector.Extent
import cats.implicits._
import org.globalforestwatch.grids.GridSources
import org.globalforestwatch.layers._

/**
  * @param gridId top left corner, padded from east ex: "10N_010E"
  */
case class TreeLossGridSources(gridId: String) extends GridSources {

  val treeCoverLoss = TreeCoverLoss(gridId)
  val treeCoverGain = TreeCoverGain(gridId)
  val treeCoverDensity2000 = TreeCoverDensityThresholds2000(gridId)
  val treeCoverDensity2010 = TreeCoverDensityThresholds2010(gridId)
  val biomassPerHectar = BiomassPerHectar(gridId)

  //  val mangroveBiomass = MangroveBiomass(gridId)
  val treeCoverLossDrivers = TreeCoverLossDrivers(gridId)
  val globalLandCover = GlobalLandcover(gridId)
  val primaryForest = PrimaryForest(gridId)
  //  val indonesiaPrimaryForest = IndonesiaPrimaryForest(gridId)
  //  val erosion = Erosion(gridId)
  //  val biodiversitySignificance = BiodiversitySignificance(gridId)
  //  val biodiversityIntactness = BiodiversityIntactness(gridId)
  val protectedAreas = ProtectedAreas(gridId)
  val aze = Aze(gridId)
  val plantations = Plantations(gridId)
  //  val riverBasins = RiverBasins(gridId)
  //  val ecozones = Ecozones(gridId)
  //  val urbanWatersheds = UrbanWatersheds(gridId)
  val mangroves1996 = Mangroves1996(gridId)
  val mangroves2016 = Mangroves2016(gridId)
  //  val waterStress = WaterStress(gridId)
  val intactForestLandscapes = IntactForestLandscapes(gridId)
  //  val endemicBirdAreas = EndemicBirdAreas(gridId)
  val tigerLandscapes = TigerLandscapes(gridId)
  val landmark = Landmark(gridId)
  val landRights = LandRights(gridId)
  val keyBiodiversityAreas = KeyBiodiversityAreas(gridId)
  val mining = Mining(gridId)
  //  val rspo = RSPO(gridId)
  val peatlands = Peatlands(gridId)
  val oilPalm = OilPalm(gridId)
  val indonesiaForestMoratorium = IndonesiaForestMoratorium(gridId)
  //  val indonesiaLandCover = IndonesiaLandCover(gridId)
  //  val indonesiaForestArea = IndonesiaForestArea(gridId)
  //  val mexicoProtectedAreas = MexicoProtectedAreas(gridId)
  //  val mexicoPaymentForEcosystemServices =
//    MexicoPaymentForEcosystemServices(gridId)
  //  val mexicoForestZoning = MexicoForestZoning(gridId)
  //  val peruProductionForest = PeruProductionForest(gridId)
  //  val peruProtectedAreas = PeruProtectedAreas(gridId)
  //  val peruForestConcessions = PeruForestConcessions(gridId)
  //  val brazilBiomes = BrazilBiomes(gridId)
  val woodFiber = WoodFiber(gridId)
  val resourceRights = ResourceRights(gridId)
  val logging = Logging(gridId)
  //  val oilGas = OilGas(gridId)

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
//      val mangroveBiomassTile = mangroveBiomass.fetchWindow(window)
      val driversTile = treeCoverLossDrivers.fetchWindow(window)
      val globalLandCoverTile = globalLandCover.fetchWindow(window)
      val primaryForestTile = primaryForest.fetchWindow(window)
//      val idnPrimaryForestTile = indonesiaPrimaryForest.fetchWindow(window)
//      val erosionTile = erosion.fetchWindow(window)
//      val biodiversitySignificanceTile =
//        biodiversitySignificance.fetchWindow(window)
//      val biodiversityIntactnessTile =
//        biodiversityIntactness.fetchWindow(window)
      val wdpaTile = protectedAreas.fetchWindow(window)
      val azeTile = aze.fetchWindow(window)
      val plantationsTile = plantations.fetchWindow(window)
//      val riverBasinsTile = riverBasins.fetchWindow(window)
//      val ecozonesTile = ecozones.fetchWindow(window)
//      val urbanWatershedsTile = urbanWatersheds.fetchWindow(window)
val mangroves1996Tile = mangroves1996.fetchWindow(window)
      val mangroves2016Tile = mangroves2016.fetchWindow(window)
//      val waterStressTile = waterStress.fetchWindow(window)
      val intactForestLandscapesTile =
        intactForestLandscapes.fetchWindow(window)
//      val endemicBirdAreasTile = endemicBirdAreas.fetchWindow(window)
      val tigerLandscapesTile = tigerLandscapes.fetchWindow(window)
      val landmarkTile = landmark.fetchWindow(window)
      val landRightsTile = landRights.fetchWindow(window)
      val keyBiodiversityAreasTile = keyBiodiversityAreas.fetchWindow(window)
      val miningTile = mining.fetchWindow(window)
//      val rspoTile = rspo.fetchWindow(window)
val peatlandsTile = peatlands.fetchWindow(window)
      val oilPalmTile = oilPalm.fetchWindow(window)
      val idnForestMoratoriumTile =
        indonesiaForestMoratorium.fetchWindow(window)
//      val idnLandCoverTile = indonesiaLandCover.fetchWindow(window)
//      val idnForestAreaTile = indonesiaForestArea.fetchWindow(window)
//      val mexProtectedAreasTile = mexicoProtectedAreas.fetchWindow(window)
//      val mexPaymentForEcosystemServicesTile =
//        mexicoPaymentForEcosystemServices.fetchWindow(window)
//      val mexForestZoningTile = mexicoForestZoning.fetchWindow(window)
//      val perProductionForestTile = peruProductionForest.fetchWindow(window)
//      val perProtectedAreasTile = peruProtectedAreas.fetchWindow(window)
//      val perForestConcessionsTile = peruForestConcessions.fetchWindow(window)
//      val braBiomesTile = brazilBiomes.fetchWindow(window)
      val woodFiberTile = woodFiber.fetchWindow(window)
      val resourceRightsTile = resourceRights.fetchWindow(window)
      val loggingTile = logging.fetchWindow(window)
//      val oilGasTile = oilGas.fetchWindow(window)

      val tile = TreeLossTile(
        lossTile,
        gainTile,
        tcd2000Tile,
        tcd2010Tile,
        biomassTile,
//        mangroveBiomassTile,
        driversTile,
        globalLandCoverTile,
        primaryForestTile,
//        idnPrimaryForestTile,
//        erosionTile,
//        biodiversitySignificanceTile,
//        biodiversityIntactnessTile,
        wdpaTile,
        azeTile,
        plantationsTile,
//        riverBasinsTile,
//        ecozonesTile,
//        urbanWatershedsTile,
        mangroves1996Tile,
        mangroves2016Tile,
//        waterStressTile,
        intactForestLandscapesTile,
//        endemicBirdAreasTile,
        tigerLandscapesTile,
        landmarkTile,
        landRightsTile,
        keyBiodiversityAreasTile,
        miningTile,
//        rspoTile,
        peatlandsTile,
        oilPalmTile,
        idnForestMoratoriumTile,
//        idnLandCoverTile,
//        mexProtectedAreasTile,
//        mexPaymentForEcosystemServicesTile,
//        mexForestZoningTile,
//        perProductionForestTile,
//        perProtectedAreasTile,
//        perForestConcessionsTile,
//        braBiomesTile,
        woodFiberTile,
        resourceRightsTile,
        loggingTile
//        oilGasTile
      )

      Raster(tile, window)
    }
  }
}

object TreeLossGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap.empty[String, TreeLossGridSources]

  def getCachedSources(gridId: String): TreeLossGridSources = {

    cache.getOrElseUpdate(gridId, TreeLossGridSources(gridId))

  }

}
