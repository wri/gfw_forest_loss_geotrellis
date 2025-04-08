package org.globalforestwatch.summarystats.carbonflux

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.Raster
import org.globalforestwatch.grids.{GridSources, GridTile}
import org.globalforestwatch.layers._

/**
  * @param gridTile top left corner, padded from east ex: "10N_010E"
  */
case class CarbonFluxGridSources(gridTile: GridTile, kwargs: Map[String, Any])
  extends GridSources {

  val treeCoverLoss: TreeCoverLoss = TreeCoverLoss(gridTile, kwargs)
  val treeCoverGain: TreeCoverGain = TreeCoverGain(gridTile, kwargs)
  val treeCoverDensity2000: TreeCoverDensityThreshold2000 = TreeCoverDensityThreshold2000(gridTile, kwargs)
  val biomassPerHectar: AbovegroundBiomass2000 = AbovegroundBiomass2000(gridTile, kwargs)
  val grossAnnualAbovegroundRemovalsCarbon: AnnualAbovegroundRemovalFactorCarbon = AnnualAbovegroundRemovalFactorCarbon(gridTile, kwargs = kwargs)
  val grossAnnualBelowgroundRemovalsCarbon: AnnualBelowgroundRemovalFactorCarbon = AnnualBelowgroundRemovalFactorCarbon(gridTile, kwargs = kwargs)
  val grossCumulAbovegroundRemovalsCo2: GrossCumulAbovegroundRemovalsCo2 = GrossCumulAbovegroundRemovalsCo2(gridTile, kwargs = kwargs)
  val grossCumulBelowgroundRemovalsCo2: GrossCumulBelowgroundRemovalsCo2 = GrossCumulBelowgroundRemovalsCo2(gridTile, kwargs = kwargs)
  val netFluxCo2: NetFluxCo2e = NetFluxCo2e(gridTile, kwargs = kwargs)
  val agcEmisYear: AbovegroundCarbonEmisYear = AbovegroundCarbonEmisYear(gridTile, kwargs = kwargs)
  val bgcEmisYear: BelowgroundCarbonEmisYear = BelowgroundCarbonEmisYear(gridTile, kwargs = kwargs)
  val deadwoodCarbonEmisYear: DeadwoodCarbonEmisYear = DeadwoodCarbonEmisYear(gridTile, kwargs = kwargs)
  val litterCarbonEmisYear: LitterCarbonEmisYear = LitterCarbonEmisYear(gridTile, kwargs = kwargs)
  val soilCarbonEmisYear: SoilCarbonEmisYear = SoilCarbonEmisYear(gridTile, kwargs = kwargs)
  val abovegroundCarbon2000: AbovegroundCarbon2000 = AbovegroundCarbon2000(gridTile, kwargs = kwargs)
  val belowgroundCarbon2000: BelowgroundCarbon2000 = BelowgroundCarbon2000(gridTile, kwargs = kwargs)
  val deadwoodCarbon2000: DeadwoodCarbon2000 = DeadwoodCarbon2000(gridTile, kwargs = kwargs)
  val litterCarbon2000: LitterCarbon2000 = LitterCarbon2000(gridTile, kwargs = kwargs)
  val soilCarbon2000: SoilCarbon2000 = SoilCarbon2000(gridTile, kwargs = kwargs)
  val grossEmissionsCo2eCh4BiomassSoil: GrossEmissionsCH4Co2eBiomassSoil = GrossEmissionsCH4Co2eBiomassSoil(gridTile, kwargs = kwargs)
  val grossEmissionsCo2eN2oBiomassSoil: GrossEmissionsN2OCo2eBiomassSoil = GrossEmissionsN2OCo2eBiomassSoil(gridTile, kwargs = kwargs)
  val grossEmissionsCo2eCo2OnlyBiomassSoil: GrossEmissionsCo2OnlyCo2BiomassSoil = GrossEmissionsCo2OnlyCo2BiomassSoil(gridTile, kwargs = kwargs)
  val grossEmissionsCo2eCh4SoilOnly: GrossEmissionsCH4Co2eSoilOnly = GrossEmissionsCH4Co2eSoilOnly(gridTile, kwargs = kwargs)
  val grossEmissionsCo2eN2oSoilOnly: GrossEmissionsN2OCo2eSoilOnly = GrossEmissionsN2OCo2eSoilOnly(gridTile, kwargs = kwargs)
  val grossEmissionsCo2eCo2OnlySoilOnly: GrossEmissionsCo2OnlyCo2SoilOnly = GrossEmissionsCo2OnlyCo2SoilOnly(gridTile, kwargs = kwargs)
  val jplTropicsAbovegroundBiomassDensity2000: JplTropicsAbovegroundBiomassDensity2000 = JplTropicsAbovegroundBiomassDensity2000(gridTile, kwargs)
  val stdevAnnualAbovegroundRemovalsCarbon: StdevAnnualAbovegroundRemovalsCarbon = StdevAnnualAbovegroundRemovalsCarbon(gridTile, kwargs = kwargs)
  val stdevSoilCarbon2000: StdevSoilCarbon2000 = StdevSoilCarbon2000(gridTile, kwargs = kwargs)

  val fluxModelExtent: ForestFluxModelExtent = ForestFluxModelExtent(gridTile, kwargs = kwargs)
  val mangroveBiomassExtent: MangroveBiomassExtent = MangroveBiomassExtent(gridTile, kwargs)
  val removalForestType: ForestFluxModelRemovalForestType = ForestFluxModelRemovalForestType(gridTile, kwargs = kwargs)
  val treeCoverLossDrivers: TreeCoverLossDrivers1km = TreeCoverLossDrivers1km(gridTile, kwargs)
  val faoEcozones2000: FaoEcozones2000 = FaoEcozones2000(gridTile, kwargs = kwargs)
  val protectedAreas: ProtectedAreas = ProtectedAreas(gridTile, kwargs)
  val landmark: Landmark = Landmark(gridTile, kwargs)
  val intactForestLandscapes2000: IntactForestLandscapes2000 = IntactForestLandscapes2000(gridTile, kwargs)
  val plantationsTypeFluxModel: ForestFluxModelPlantedForestType = ForestFluxModelPlantedForestType(gridTile, kwargs)
  val intactPrimaryForest: IntactPrimaryForest = IntactPrimaryForest(gridTile, kwargs)
  val peatlands: Peatlands = Peatlands(gridTile, kwargs)
  val forestAgeCategory: ForestFluxModelAgeCategory = ForestFluxModelAgeCategory(gridTile, kwargs = kwargs)
  val jplTropicsAbovegroundBiomassExtent2000: JplTropicsAbovegroundBiomassExtent2000 = JplTropicsAbovegroundBiomassExtent2000(gridTile, kwargs)
  val fiaRegionsUsExtent: FiaRegionsUsExtent = FiaRegionsUsExtent(gridTile, kwargs)
  val brazilBiomes: BrazilBiomes = BrazilBiomes(gridTile, kwargs)
  val riverBasins: RiverBasins = RiverBasins(gridTile, kwargs)
  val primaryForest: PrimaryForest = PrimaryForest(gridTile, kwargs)
  val treeCoverLossLegalAmazon: TreeCoverLossLegalAmazon = TreeCoverLossLegalAmazon(gridTile, kwargs)
  val prodesLegalAmazonExtent2000: ProdesLegalAmazonExtent2000 = ProdesLegalAmazonExtent2000(gridTile, kwargs)
  val tropicLatitudeExtent: TropicLatitudeExtent = TropicLatitudeExtent(gridTile, kwargs)
  val treeCoverLossFromFires: TreeCoverLossFromFires = TreeCoverLossFromFires(gridTile, kwargs)
  val grossEmissionsNodeCodes: ForestFluxModelGrossEmissionsNodeCodes = ForestFluxModelGrossEmissionsNodeCodes(gridTile, kwargs = kwargs)
  val plantationsPre2000: PlantationsPre2000 = PlantationsPre2000(gridTile, kwargs)
  val keyBiodiversityAreas: KeyBiodiversityAreas = KeyBiodiversityAreas(gridTile, kwargs)

  def readWindow(
                  windowKey: SpatialKey,
                  windowLayout: LayoutDefinition
                ): Either[Throwable, Raster[CarbonFluxTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      lossTile <- Either
        .catchNonFatal(treeCoverLoss.fetchWindow(windowKey, windowLayout))
        .right
      tcd2000Tile <- Either
        .catchNonFatal(
          treeCoverDensity2000.fetchWindow(windowKey, windowLayout)
        )
        .right

    } yield {
      // Failure for these will be converted to optional result and propagated with TreeLossTile
      val gainTile = treeCoverGain.fetchWindow(windowKey, windowLayout)
      val biomassTile = biomassPerHectar.fetchWindow(windowKey, windowLayout)
      val grossAnnualAbovegroundRemovalsCarbonTile = grossAnnualAbovegroundRemovalsCarbon.fetchWindow(windowKey, windowLayout)
      val grossAnnualBelowgroundRemovalsCarbonTile = grossAnnualBelowgroundRemovalsCarbon.fetchWindow(windowKey, windowLayout)
      val grossCumulAbovegroundRemovalsCo2Tile = grossCumulAbovegroundRemovalsCo2.fetchWindow(windowKey, windowLayout)
      val grossCumulBelowgroundRemovalsCo2Tile = grossCumulBelowgroundRemovalsCo2.fetchWindow(windowKey, windowLayout)
      val netFluxCo2Tile = netFluxCo2.fetchWindow(windowKey, windowLayout)
      val agcEmisYearTile = agcEmisYear.fetchWindow(windowKey, windowLayout)
      val bgcEmisYearTile = bgcEmisYear.fetchWindow(windowKey, windowLayout)
      val deadwoodCarbonEmisYearTile = deadwoodCarbonEmisYear.fetchWindow(windowKey, windowLayout)
      val litterCarbonEmisYearTile = litterCarbonEmisYear.fetchWindow(windowKey, windowLayout)
      val soilCarbonEmisYearTile = soilCarbonEmisYear.fetchWindow(windowKey, windowLayout)
      val abovegroundCarbon2000Tile = abovegroundCarbon2000.fetchWindow(windowKey, windowLayout)
      val belowgroundCarbon2000Tile = belowgroundCarbon2000.fetchWindow(windowKey, windowLayout)
      val deadwoodCarbon2000Tile = deadwoodCarbon2000.fetchWindow(windowKey, windowLayout)
      val litterCarbon2000Tile = litterCarbon2000.fetchWindow(windowKey, windowLayout)
      val soilCarbon2000Tile = soilCarbon2000.fetchWindow(windowKey, windowLayout)
      val grossEmissionsCo2eCh4BiomassSoilTile = grossEmissionsCo2eCh4BiomassSoil.fetchWindow(windowKey, windowLayout)
      val grossEmissionsCo2eN2oBiomassSoilTile = grossEmissionsCo2eN2oBiomassSoil.fetchWindow(windowKey, windowLayout)
      val grossEmissionsCo2eCo2OnlyBiomassSoilTile = grossEmissionsCo2eCo2OnlyBiomassSoil.fetchWindow(windowKey, windowLayout)
      val grossEmissionsCo2eCh4SoilOnlyTile = grossEmissionsCo2eCh4SoilOnly.fetchWindow(windowKey, windowLayout)
      val grossEmissionsCo2eN2oSoilOnlyTile = grossEmissionsCo2eN2oSoilOnly.fetchWindow(windowKey, windowLayout)
      val grossEmissionsCo2eCo2OnlySoilOnlyTile = grossEmissionsCo2eCo2OnlySoilOnly.fetchWindow(windowKey, windowLayout)
      val jplTropicsAbovegroundBiomassDensity2000Tile = jplTropicsAbovegroundBiomassDensity2000.fetchWindow(windowKey, windowLayout)
      val stdevAnnualAbovegroundRemovalsCarbonTile = stdevAnnualAbovegroundRemovalsCarbon.fetchWindow(windowKey, windowLayout)
      val stdevSoilCarbon2000Tile = stdevSoilCarbon2000.fetchWindow(windowKey, windowLayout)

      val fluxModelExtentTile = fluxModelExtent.fetchWindow(windowKey, windowLayout)
      val mangroveBiomassExtentTile = mangroveBiomassExtent.fetchWindow(windowKey, windowLayout)
      val removalForestTypeTile = removalForestType.fetchWindow(windowKey, windowLayout)
      val driversTile = treeCoverLossDrivers.fetchWindow(windowKey, windowLayout)
      val faoEcozones2000Tile = faoEcozones2000.fetchWindow(windowKey, windowLayout)
      val wdpaTile = protectedAreas.fetchWindow(windowKey, windowLayout)
      val landmarkTile = landmark.fetchWindow(windowKey, windowLayout)
      val intactForestLandscapes2000Tile = intactForestLandscapes2000.fetchWindow(windowKey, windowLayout)
      val plantationsTypeFluxTile = plantationsTypeFluxModel.fetchWindow(windowKey, windowLayout)
      val intactPrimaryForestTile = intactPrimaryForest.fetchWindow(windowKey, windowLayout)
      val peatlandsTile = peatlands.fetchWindow(windowKey, windowLayout)
      val forestAgeCategoryTile = forestAgeCategory.fetchWindow(windowKey, windowLayout)
      val jplTropicsAbovegroundBiomassExtent2000Tile = jplTropicsAbovegroundBiomassExtent2000.fetchWindow(windowKey, windowLayout)
      val fiaRegionsUsExtentTile = fiaRegionsUsExtent.fetchWindow(windowKey, windowLayout)
      val brazilBiomesTile = brazilBiomes.fetchWindow(windowKey, windowLayout)
      val riverBasinsTile = riverBasins.fetchWindow(windowKey, windowLayout)
      val primaryForestTile = primaryForest.fetchWindow(windowKey, windowLayout)
      val treeCoverLossLegalAmazonTile = treeCoverLossLegalAmazon.fetchWindow(windowKey, windowLayout)
      val prodesLegalAmazonExtent2000Tile = prodesLegalAmazonExtent2000.fetchWindow(windowKey, windowLayout)
      val tropicLatitudeExtentTile = tropicLatitudeExtent.fetchWindow(windowKey, windowLayout)
      val treeCoverLossFromFiresTile = treeCoverLossFromFires.fetchWindow(windowKey, windowLayout)
      val grossEmissionsNodeCodesTile = grossEmissionsNodeCodes.fetchWindow(windowKey, windowLayout)
      val plantationsPre2000Tile = plantationsPre2000.fetchWindow(windowKey, windowLayout)
      val keyBiodiversityAreasTile = keyBiodiversityAreas.fetchWindow(windowKey, windowLayout)


      val tile = CarbonFluxTile(
        lossTile,
        gainTile,
        tcd2000Tile,
        biomassTile,
        grossAnnualAbovegroundRemovalsCarbonTile,
        grossAnnualBelowgroundRemovalsCarbonTile,
        grossCumulAbovegroundRemovalsCo2Tile,
        grossCumulBelowgroundRemovalsCo2Tile,
        netFluxCo2Tile,
        agcEmisYearTile,
        bgcEmisYearTile,
        deadwoodCarbonEmisYearTile,
        litterCarbonEmisYearTile,
        soilCarbonEmisYearTile,
        abovegroundCarbon2000Tile,
        belowgroundCarbon2000Tile,
        deadwoodCarbon2000Tile,
        litterCarbon2000Tile,
        soilCarbon2000Tile,
        grossEmissionsCo2eCh4BiomassSoilTile,
        grossEmissionsCo2eN2oBiomassSoilTile,
        grossEmissionsCo2eCo2OnlyBiomassSoilTile,
        grossEmissionsCo2eCh4SoilOnlyTile,
        grossEmissionsCo2eN2oSoilOnlyTile,
        grossEmissionsCo2eCo2OnlySoilOnlyTile,
        jplTropicsAbovegroundBiomassDensity2000Tile,
        stdevAnnualAbovegroundRemovalsCarbonTile,
        stdevSoilCarbon2000Tile,

        fluxModelExtentTile,
        mangroveBiomassExtentTile,
        removalForestTypeTile,
        driversTile,
        faoEcozones2000Tile,
        landmarkTile,
        wdpaTile,
        intactForestLandscapes2000Tile,
        plantationsTypeFluxTile,
        intactPrimaryForestTile,
        peatlandsTile,
        forestAgeCategoryTile,
        jplTropicsAbovegroundBiomassExtent2000Tile,
        fiaRegionsUsExtentTile,
        brazilBiomesTile,
        riverBasinsTile,
        primaryForestTile,
        treeCoverLossLegalAmazonTile,
        prodesLegalAmazonExtent2000Tile,
        tropicLatitudeExtentTile,
        treeCoverLossFromFiresTile,
        grossEmissionsNodeCodesTile,
        plantationsPre2000Tile,
        keyBiodiversityAreasTile
      )

      Raster(tile, windowKey.extent(windowLayout))
    }
  }
}

object CarbonFluxGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap.empty[String, CarbonFluxGridSources]

  def getCachedSources(gridTile: GridTile,
                       kwargs: Map[String, Any]): CarbonFluxGridSources = {

    cache.getOrElseUpdate(
      gridTile.tileId,
      CarbonFluxGridSources(gridTile, kwargs)
    )

  }

}
