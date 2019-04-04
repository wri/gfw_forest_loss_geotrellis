package org.globalforestwatch.treecoverloss


import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.{Raster, Tile}
import geotrellis.vector.Extent
import cats.implicits._
import org.globalforestwatch.layers._

/**
  * @param grid top left corner, padded from east ex: "10N_010E"
  */
case class TenByTenGridSources(grid: String) extends LazyLogging {

  def readWindow(window: Extent): Either[Throwable, Raster[TreeLossTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      loss <- Either.catchNonFatal(new TreeCoverLoss(grid).fetchWindow(window)).right
      gain <- Either.catchNonFatal(new TreeCoverGain(grid).fetchWindow(window)).right
      tcd2000 <- Either.catchNonFatal(new TreeCoverDensity2000(grid).fetchWindow(window)).right
      tcd2010 <- Either.catchNonFatal(new TreeCoverDensity2010(grid).fetchWindow(window)).right
      co2Pixel <- Either.catchNonFatal(new Carbon(grid).fetchWindow(window)).right
      biomass <- Either.catchNonFatal(new BiomassPerHectar(grid).fetchWindow(window)).right

    } yield {
      // Failure for these will be converted to optional result and propagated with TreeLossTile
      val mangroveBiomass = new MangroveBiomass(grid).fetchWindow(window)
      val drivers = new TreeCoverLossDrivers(grid).fetchWindow(window)
      val globalLandCover = new GlobalLandcover(grid).fetchWindow(window)
      val primaryForest = new PrimaryForest(grid).fetchWindow(window)
      val idnPrimaryForest = new IndonesiaPrimaryForest(grid).fetchWindow(window)
      val erosion = new Erosion(grid).fetchWindow(window)
      val biodiversitySignificance = new BiodiversitySignificance(grid).fetchWindow(window)
      val wdpa = new ProtectedAreas(grid).fetchWindow(window)
      val plantations = new Plantations(grid).fetchWindow(window)
      val riverBasins = new RiverBasins(grid).fetchWindow(window)
      val ecozones = new Ecozones(grid).fetchWindow(window)
      val urbanWatersheds = new UrbanWatersheds(grid).fetchWindow(window)
      val mangroves1996 = new Mangroves1996(grid).fetchWindow(window)
      val mangroves2016 = new Mangroves2016(grid).fetchWindow(window)
      val waterStress = new WaterStress(grid).fetchWindow(window)
      val intactForestLandscapes = new IntactForestLandscapes(grid).fetchWindow(window)
      val endemicBirdAreas = new EndemicBirdAreas(grid).fetchWindow(window)
      val tigerLandscapes = new TigerLandscapes(grid).fetchWindow(window)
      val landmark = new Landmark(grid).fetchWindow(window)
      val landRights = new LandRights(grid).fetchWindow(window)
      val keyBiodiversityAreas = new KeyBiodiversityAreas(grid).fetchWindow(window)
      val mining = new Mining(grid).fetchWindow(window)
      val rspo = new RSPO(grid).fetchWindow(window)
      val peatlands = new Peatlands(grid).fetchWindow(window)
      val oilPalm = new OilPalm(grid).fetchWindow(window)
      val idnForestMoratorium = new IndonesiaForestMoratorium(grid).fetchWindow(window)
      val idnLandCover = new IndonesiaLandCover(grid).fetchWindow(window)
      val idnForestArea = new IndonesiaForestArea(grid).fetchWindow(window)
      val mexProtectedAreas = new MexicoProtectedAreas(grid).fetchWindow(window)
      val mexPaymentForEcosystemServices = new MexicoPaymentForEcosystemServices(grid).fetchWindow(window)
      val mexForestZoning = new MexicoForestZoning(grid).fetchWindow(window)
      val perProductionForest = new PeruProductionForest(grid).fetchWindow(window)
      val perProtectedAreas = new PeruProtectedAreas(grid).fetchWindow(window)
      val perForestConcessions = new PeruForestConcessions(grid).fetchWindow(window)
      val braBiomes = new BrazilBiomes(grid).fetchWindow(window)
      val woodFiber = new WoodFiber(grid).fetchWindow(window)
      val resourceRights = new ResourceRights(grid).fetchWindow(window)
      val logging = new Logging(grid).fetchWindow(window)

      val tile = TreeLossTile(
        loss,
        gain,
        tcd2000,
        tcd2010,
        co2Pixel,
        biomass,
        mangroveBiomass,
        drivers,
        globalLandCover,
        primaryForest,
        idnPrimaryForest,
        erosion,
        biodiversitySignificance,
        wdpa,
        plantations,
        riverBasins,
        ecozones,
        urbanWatersheds,
        mangroves1996,
        mangroves2016,
        waterStress,
        intactForestLandscapes,
        endemicBirdAreas,
        tigerLandscapes,
        landmark,
        landRights,
        keyBiodiversityAreas,
        mining,
        rspo,
        peatlands,
        oilPalm,
        idnForestMoratorium,
        idnLandCover,
        mexProtectedAreas,
        mexPaymentForEcosystemServices,
        mexForestZoning,
        perProductionForest,
        perProtectedAreas,
        perForestConcessions,
        braBiomes,
        woodFiber,
        resourceRights,
        logging
      )

      Raster(tile, window)
    }
  }
}
