package org.globalforestwatch.summarystats.annualupdate

import cats.implicits._
import geotrellis.raster.Raster
import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridSources, GridTile}
import org.globalforestwatch.layers._

/**
  * @param gridTile top left corner, padded from east ex: "10N_010E"
  */
case class AnnualUpdateGridSources(gridTile: GridTile) extends GridSources {

  val treeCoverLoss = TreeCoverLoss(gridTile)
  val treeCoverGain = TreeCoverGain(gridTile)
  val treeCoverDensity2000 = TreeCoverDensityPercent2000(gridTile)
  val treeCoverDensity2010 = TreeCoverDensityPercent2010(gridTile)
  val biomassPerHectar = BiomassPerHectar(gridTile)

  val mangroveBiomass = MangroveBiomass(gridTile)
  val treeCoverLossDrivers = TreeCoverLossDrivers(gridTile)
  val globalLandCover = GlobalLandcover(gridTile)
  val primaryForest = PrimaryForest(gridTile)
  val indonesiaPrimaryForest = IndonesiaPrimaryForest(gridTile)
  val erosion = Erosion(gridTile)
  val biodiversitySignificance = BiodiversitySignificance(gridTile)
  val biodiversityIntactness = BiodiversityIntactness(gridTile)
  val protectedAreas = ProtectedAreas(gridTile)
  val aze = Aze(gridTile)
  val plantations = Plantations(gridTile)
  val riverBasins = RiverBasins(gridTile)
  val ecozones = Ecozones(gridTile)
  val urbanWatersheds = UrbanWatersheds(gridTile)
  val mangroves1996 = Mangroves1996(gridTile)
  val mangroves2016 = Mangroves2016(gridTile)
  val waterStress = WaterStress(gridTile)
  val intactForestLandscapes = IntactForestLandscapes(gridTile)
  val endemicBirdAreas = EndemicBirdAreas(gridTile)
  val tigerLandscapes = TigerLandscapes(gridTile)
  val landmark = Landmark(gridTile)
  val landRights = LandRights(gridTile)
  val keyBiodiversityAreas = KeyBiodiversityAreas(gridTile)
  val mining = Mining(gridTile)
  val rspo = RSPO(gridTile)
  val peatlands = Peatlands(gridTile)
  val oilPalm = OilPalm(gridTile)
  val indonesiaForestMoratorium = IndonesiaForestMoratorium(gridTile)
  val indonesiaLandCover = IndonesiaLandCover(gridTile)
  val indonesiaForestArea = IndonesiaForestArea(gridTile)
  val mexicoProtectedAreas = MexicoProtectedAreas(gridTile)
  val mexicoPaymentForEcosystemServices =
    MexicoPaymentForEcosystemServices(gridTile)
  val mexicoForestZoning = MexicoForestZoning(gridTile)
  val peruProductionForest = PeruProductionForest(gridTile)
  val peruProtectedAreas = PeruProtectedAreas(gridTile)
  val peruForestConcessions = PeruForestConcessions(gridTile)
  val brazilBiomes = BrazilBiomes(gridTile)
  val woodFiber = WoodFiber(gridTile)
  val resourceRights = ResourceRights(gridTile)
  val logging = Logging(gridTile)
  val oilGas = OilGas(gridTile)

  def readWindow(
    window: Extent
  ): Either[Throwable, Raster[AnnualUpdateTile]] = {

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
      biomassTile <- Either
        .catchNonFatal(biomassPerHectar.fetchWindow(window))
        .right

    } yield {
      // Failure for these will be converted to optional result and propagated with TreeLossTile
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

      val tile = AnnualUpdateTile(
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

object AnnualUpdateGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap.empty[String, AnnualUpdateGridSources]

  def getCachedSources(gridTile: GridTile): AnnualUpdateGridSources = {

    cache.getOrElseUpdate(gridTile.tileId, AnnualUpdateGridSources(gridTile))

  }

}
