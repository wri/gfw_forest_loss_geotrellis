package org.globalforestwatch.summarystats.annualupdate

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
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
                  windowKey: SpatialKey, windowLayout: LayoutDefinition
  ): Either[Throwable, Raster[AnnualUpdateTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      lossTile <- Either.catchNonFatal(treeCoverLoss.fetchWindow(windowKey, windowLayout)).right
      gainTile <- Either.catchNonFatal(treeCoverGain.fetchWindow(windowKey, windowLayout)).right
      tcd2000Tile <- Either
        .catchNonFatal(treeCoverDensity2000.fetchWindow(windowKey, windowLayout))
        .right
      tcd2010Tile <- Either
        .catchNonFatal(treeCoverDensity2010.fetchWindow(windowKey, windowLayout))
        .right
      biomassTile <- Either
        .catchNonFatal(biomassPerHectar.fetchWindow(windowKey, windowLayout))
        .right

    } yield {
      // Failure for these will be converted to optional result and propagated with TreeLossTile
      val mangroveBiomassTile = mangroveBiomass.fetchWindow(windowKey, windowLayout)
      val driversTile = treeCoverLossDrivers.fetchWindow(windowKey, windowLayout)
      val globalLandCoverTile = globalLandCover.fetchWindow(windowKey, windowLayout)
      val primaryForestTile = primaryForest.fetchWindow(windowKey, windowLayout)
      val idnPrimaryForestTile = indonesiaPrimaryForest.fetchWindow(windowKey, windowLayout)
      val erosionTile = erosion.fetchWindow(windowKey, windowLayout)
      val biodiversitySignificanceTile =
        biodiversitySignificance.fetchWindow(windowKey, windowLayout)
      val biodiversityIntactnessTile =
        biodiversityIntactness.fetchWindow(windowKey, windowLayout)
      val wdpaTile = protectedAreas.fetchWindow(windowKey, windowLayout)
      val azeTile = aze.fetchWindow(windowKey, windowLayout)
      val plantationsTile = plantations.fetchWindow(windowKey, windowLayout)
      val riverBasinsTile = riverBasins.fetchWindow(windowKey, windowLayout)
      val ecozonesTile = ecozones.fetchWindow(windowKey, windowLayout)
      val urbanWatershedsTile = urbanWatersheds.fetchWindow(windowKey, windowLayout)
      val mangroves1996Tile = mangroves1996.fetchWindow(windowKey, windowLayout)
      val mangroves2016Tile = mangroves2016.fetchWindow(windowKey, windowLayout)
      val waterStressTile = waterStress.fetchWindow(windowKey, windowLayout)
      val intactForestLandscapesTile =
        intactForestLandscapes.fetchWindow(windowKey, windowLayout)
      val endemicBirdAreasTile = endemicBirdAreas.fetchWindow(windowKey, windowLayout)
      val tigerLandscapesTile = tigerLandscapes.fetchWindow(windowKey, windowLayout)
      val landmarkTile = landmark.fetchWindow(windowKey, windowLayout)
      val landRightsTile = landRights.fetchWindow(windowKey, windowLayout)
      val keyBiodiversityAreasTile = keyBiodiversityAreas.fetchWindow(windowKey, windowLayout)
      val miningTile = mining.fetchWindow(windowKey, windowLayout)
      val rspoTile = rspo.fetchWindow(windowKey, windowLayout)
      val peatlandsTile = peatlands.fetchWindow(windowKey, windowLayout)
      val oilPalmTile = oilPalm.fetchWindow(windowKey, windowLayout)
      val idnForestMoratoriumTile =
        indonesiaForestMoratorium.fetchWindow(windowKey, windowLayout)
      val idnLandCoverTile = indonesiaLandCover.fetchWindow(windowKey, windowLayout)
      val idnForestAreaTile = indonesiaForestArea.fetchWindow(windowKey, windowLayout)
      val mexProtectedAreasTile = mexicoProtectedAreas.fetchWindow(windowKey, windowLayout)
      val mexPaymentForEcosystemServicesTile =
        mexicoPaymentForEcosystemServices.fetchWindow(windowKey, windowLayout)
      val mexForestZoningTile = mexicoForestZoning.fetchWindow(windowKey, windowLayout)
      val perProductionForestTile = peruProductionForest.fetchWindow(windowKey, windowLayout)
      val perProtectedAreasTile = peruProtectedAreas.fetchWindow(windowKey, windowLayout)
      val perForestConcessionsTile = peruForestConcessions.fetchWindow(windowKey, windowLayout)
      val braBiomesTile = brazilBiomes.fetchWindow(windowKey, windowLayout)
      val woodFiberTile = woodFiber.fetchWindow(windowKey, windowLayout)
      val resourceRightsTile = resourceRights.fetchWindow(windowKey, windowLayout)
      val loggingTile = logging.fetchWindow(windowKey, windowLayout)
      val oilGasTile = oilGas.fetchWindow(windowKey, windowLayout)

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

      Raster(tile, windowKey.extent(windowLayout))
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
