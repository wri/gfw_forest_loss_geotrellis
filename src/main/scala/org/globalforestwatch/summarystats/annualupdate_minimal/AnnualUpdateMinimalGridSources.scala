package org.globalforestwatch.summarystats.annualupdate_minimal

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.Raster
import org.globalforestwatch.grids.{GridSources, GridTile}
import org.globalforestwatch.layers._

/**
  * @param gridTile top left corner, padded from east ex: "10N_010E"
  */
case class AnnualUpdateMinimalGridSources(gridTile: GridTile, kwargs: Map[String, Any]) extends GridSources {

  val treeCoverLoss: TreeCoverLoss = TreeCoverLoss(gridTile, kwargs)
  val treeCoverGain: TreeCoverGain = TreeCoverGain(gridTile, kwargs)
  val treeCoverDensity2000: TreeCoverDensityThreshold2000 = TreeCoverDensityThreshold2000(gridTile, kwargs)
  val treeCoverDensity2010: TreeCoverDensityThreshold2010 = TreeCoverDensityThreshold2010(gridTile, kwargs)
  val biomassPerHectar: BiomassPerHectar = BiomassPerHectar(gridTile, kwargs)
  val treeCoverLossDrivers = TreeCoverLossDrivers(gridTile, kwargs)
  val primaryForest: PrimaryForest = PrimaryForest(gridTile, kwargs)
  val protectedAreas: ProtectedAreas = ProtectedAreas(gridTile, kwargs)
  val aze: Aze = Aze(gridTile, kwargs)
  val plantedForests: PlantedForests = PlantedForests(gridTile, kwargs)
  val mangroves1996: Mangroves1996 = Mangroves1996(gridTile, kwargs)
  val mangroves2016: Mangroves2016 = Mangroves2016(gridTile, kwargs)
  val tigerLandscapes: TigerLandscapes = TigerLandscapes(gridTile, kwargs)
  val landmark: Landmark = Landmark(gridTile, kwargs)
  val keyBiodiversityAreas: KeyBiodiversityAreas = KeyBiodiversityAreas(gridTile, kwargs)
  val mining: Mining = Mining(gridTile, kwargs)
  val peatlands: Peatlands = Peatlands(gridTile, kwargs)
  val oilPalm: OilPalm = OilPalm(gridTile, kwargs)
  val indonesiaForestMoratorium: IndonesiaForestMoratorium = IndonesiaForestMoratorium(gridTile, kwargs)
  val woodFiber: WoodFiber = WoodFiber(gridTile, kwargs)
  val resourceRights: ResourceRights = ResourceRights(gridTile, kwargs)
  val logging: Logging = Logging(gridTile, kwargs)
  val grossCumulAbovegroundRemovalsCo2: GrossCumulAbovegroundRemovalsCo2 = GrossCumulAbovegroundRemovalsCo2(gridTile, kwargs = kwargs)
  val grossCumulBelowgroundRemovalsCo2: GrossCumulBelowgroundRemovalsCo2 = GrossCumulBelowgroundRemovalsCo2(gridTile, kwargs = kwargs)
  val netFluxCo2: NetFluxCo2e = NetFluxCo2e(gridTile, kwargs = kwargs)
  val grossEmissionsCo2eNonCo2: GrossEmissionsNonCo2Co2e = GrossEmissionsNonCo2Co2e(gridTile, kwargs = kwargs)
  val grossEmissionsCo2eCo2Only: GrossEmissionsCo2OnlyCo2e = GrossEmissionsCo2OnlyCo2e(gridTile, kwargs = kwargs)
  val soilCarbon: SoilCarbon = SoilCarbon(gridTile, kwargs = kwargs)
  val intactForestLandscapes2000: IntactForestLandscapes2000 = IntactForestLandscapes2000(gridTile, kwargs)
  val treeCoverLossFromFires: TreeCoverLossFromFires = TreeCoverLossFromFires(gridTile, kwargs)
  val treesInMosaicLandscapes: TreesInMosaicLandscapes = TreesInMosaicLandscapes(gridTile, kwargs)
  val umdGlobalLandCover: UmdGlobalLandcover = UmdGlobalLandcover(gridTile, kwargs)
  val plantationsPre2000: PlantationsPre2000 = PlantationsPre2000(gridTile, kwargs)
  val abovegroundCarbon2000: AbovegroundCarbon2000 = AbovegroundCarbon2000(gridTile, kwargs = kwargs)
  val belowgroundCarbon2000: BelowgroundCarbon2000 = BelowgroundCarbon2000(gridTile, kwargs = kwargs)

  def readWindow(
                  windowKey: SpatialKey, windowLayout: LayoutDefinition
                ): Either[Throwable, Raster[AnnualUpdateMinimalTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      lossTile <- Either.catchNonFatal(treeCoverLoss.fetchWindow(windowKey, windowLayout)).right
      tcd2000Tile <- Either
        .catchNonFatal(treeCoverDensity2000.fetchWindow(windowKey, windowLayout))
        .right
      tcd2010Tile <- Either
        .catchNonFatal(treeCoverDensity2010.fetchWindow(windowKey, windowLayout))
        .right

    } yield {
      // Failure for these will be converted to optional result and propagated with TreeLossTile
      val gainTile = treeCoverGain.fetchWindow(windowKey, windowLayout)
      val biomassTile = biomassPerHectar.fetchWindow(windowKey, windowLayout)
      val driversTile = treeCoverLossDrivers.fetchWindow(windowKey, windowLayout)
      val primaryForestTile = primaryForest.fetchWindow(windowKey, windowLayout)
      val wdpaTile = protectedAreas.fetchWindow(windowKey, windowLayout)
      val azeTile = aze.fetchWindow(windowKey, windowLayout)
      val plantedForestsTile = plantedForests.fetchWindow(windowKey, windowLayout)
      val mangroves1996Tile = mangroves1996.fetchWindow(windowKey, windowLayout)
      val mangroves2016Tile = mangroves2016.fetchWindow(windowKey, windowLayout)
      val tigerLandscapesTile = tigerLandscapes.fetchWindow(windowKey, windowLayout)
      val landmarkTile = landmark.fetchWindow(windowKey, windowLayout)
      val keyBiodiversityAreasTile = keyBiodiversityAreas.fetchWindow(windowKey, windowLayout)
      val miningTile = mining.fetchWindow(windowKey, windowLayout)
      val peatlandsTile = peatlands.fetchWindow(windowKey, windowLayout)
      val oilPalmTile = oilPalm.fetchWindow(windowKey, windowLayout)
      val idnForestMoratoriumTile = indonesiaForestMoratorium.fetchWindow(windowKey, windowLayout)
      val woodFiberTile = woodFiber.fetchWindow(windowKey, windowLayout)
      val resourceRightsTile = resourceRights.fetchWindow(windowKey, windowLayout)
      val loggingTile = logging.fetchWindow(windowKey, windowLayout)
      val grossCumulAbovegroundRemovalsCo2Tile = grossCumulAbovegroundRemovalsCo2.fetchWindow(windowKey, windowLayout)
      val grossCumulBelowgroundRemovalsCo2Tile = grossCumulBelowgroundRemovalsCo2.fetchWindow(windowKey, windowLayout)
      val netFluxCo2Tile = netFluxCo2.fetchWindow(windowKey, windowLayout)
      val grossEmissionsCo2eNonCo2Tile = grossEmissionsCo2eNonCo2.fetchWindow(windowKey, windowLayout)
      val grossEmissionsCo2eCo2OnlyTile = grossEmissionsCo2eCo2Only.fetchWindow(windowKey, windowLayout)
      val soilCarbonTile = soilCarbon.fetchWindow(windowKey, windowLayout)
      val intactForestLandscapes2000Tile = intactForestLandscapes2000.fetchWindow(windowKey, windowLayout)
      val treeCoverLossFromFiresTile = treeCoverLossFromFires.fetchWindow(windowKey, windowLayout)
      val treesInMosaicLandscapesTile = treesInMosaicLandscapes.fetchWindow(windowKey, windowLayout)
      val umdGlobalLandCoverTile = umdGlobalLandCover.fetchWindow(windowKey, windowLayout)
      val plantationsPre2000Tile = plantationsPre2000.fetchWindow(windowKey, windowLayout)
      val abovegroundCarbon2000Tile = abovegroundCarbon2000.fetchWindow(windowKey, windowLayout)
      val belowgroundCarbon2000Tile = belowgroundCarbon2000.fetchWindow(windowKey, windowLayout)

      val tile = AnnualUpdateMinimalTile(
        lossTile,
        gainTile,
        tcd2000Tile,
        tcd2010Tile,
        biomassTile,
        driversTile,
        primaryForestTile,
        wdpaTile,
        azeTile,
        plantedForestsTile,
        mangroves1996Tile,
        mangroves2016Tile,
        tigerLandscapesTile,
        landmarkTile,
        keyBiodiversityAreasTile,
        miningTile,
        peatlandsTile,
        oilPalmTile,
        idnForestMoratoriumTile,
        woodFiberTile,
        resourceRightsTile,
        loggingTile,
        grossEmissionsCo2eNonCo2Tile,
        grossEmissionsCo2eCo2OnlyTile,
        grossCumulAbovegroundRemovalsCo2Tile,
        grossCumulBelowgroundRemovalsCo2Tile,
        netFluxCo2Tile,
        soilCarbonTile,
        intactForestLandscapes2000Tile,
        treeCoverLossFromFiresTile,
        treesInMosaicLandscapesTile,
        umdGlobalLandCoverTile,
        plantationsPre2000Tile,
        abovegroundCarbon2000Tile,
        belowgroundCarbon2000Tile,
      )

      Raster(tile, windowKey.extent(windowLayout))
    }
  }
}

object AnnualUpdateMinimalGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap
      .empty[String, AnnualUpdateMinimalGridSources]

  def getCachedSources(gridTile: GridTile, kwargs: Map[String, Any]): AnnualUpdateMinimalGridSources = {

    cache.getOrElseUpdate(gridTile.tileId, AnnualUpdateMinimalGridSources(gridTile, kwargs: Map[String, Any]))

  }

}
