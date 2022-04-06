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
  val treeCoverDensity2000: TreeCoverDensityThreshold2000 = TreeCoverDensityThreshold2000(gridTile, kwargs)
  val biomassPerHectar: BiomassPerHectar = BiomassPerHectar(gridTile, kwargs)
  val grossAnnualAbovegroundRemovalsCarbon: GrossAnnualAbovegroundRemovalsCarbon =
    GrossAnnualAbovegroundRemovalsCarbon(gridTile, kwargs = kwargs)
  val grossAnnualBelowgroundRemovalsCarbon: GrossAnnualBelowgroundRemovalsCarbon =
    GrossAnnualBelowgroundRemovalsCarbon(gridTile, kwargs = kwargs)
  val grossCumulAbovegroundRemovalsCo2: GrossCumulAbovegroundRemovalsCo2 =
    GrossCumulAbovegroundRemovalsCo2(gridTile, kwargs = kwargs)
  val grossCumulBelowgroundRemovalsCo2: GrossCumulBelowgroundRemovalsCo2 =
    GrossCumulBelowgroundRemovalsCo2(gridTile, kwargs = kwargs)
  val netFluxCo2: NetFluxCo2e = NetFluxCo2e(gridTile, kwargs = kwargs)
  val agcEmisYear: AgcEmisYear = AgcEmisYear(gridTile, kwargs = kwargs)
  val bgcEmisYear: BgcEmisYear = BgcEmisYear(gridTile, kwargs = kwargs)
  val deadwoodCarbonEmisYear: DeadwoodCarbonEmisYear = DeadwoodCarbonEmisYear(gridTile, kwargs = kwargs)
  val litterCarbonEmisYear: LitterCarbonEmisYear = LitterCarbonEmisYear(gridTile, kwargs = kwargs)
  val soilCarbonEmisYear: SoilCarbonEmisYear = SoilCarbonEmisYear(gridTile, kwargs = kwargs)
  val agc2000: Agc2000 = Agc2000(gridTile, kwargs = kwargs)
  val bgc2000: Bgc2000 = Bgc2000(gridTile, kwargs = kwargs)
  val deadwoodCarbon2000: DeadwoodCarbon2000 = DeadwoodCarbon2000(gridTile, kwargs = kwargs)
  val litterCarbon2000: LitterCarbon2000 = LitterCarbon2000(gridTile, kwargs = kwargs)
  val soilCarbon2000: SoilCarbon2000 = SoilCarbon2000(gridTile, kwargs = kwargs)
  val grossEmissionsCo2eNonCo2BiomassSoil: GrossEmissionsNonCo2Co2eBiomassSoil =
    GrossEmissionsNonCo2Co2eBiomassSoil(gridTile, kwargs = kwargs)
  val grossEmissionsCo2eCo2OnlyBiomassSoil: GrossEmissionsCo2OnlyCo2eBiomassSoil =
    GrossEmissionsCo2OnlyCo2eBiomassSoil(gridTile, kwargs = kwargs)
  val grossEmissionsCo2eNonCo2SoilOnly: GrossEmissionsNonCo2Co2eSoilOnly =
    GrossEmissionsNonCo2Co2eSoilOnly(gridTile, kwargs = kwargs)
  val grossEmissionsCo2eCo2OnlySoilOnly: GrossEmissionsCo2OnlyCo2eSoilOnly =
    GrossEmissionsCo2OnlyCo2eSoilOnly(gridTile, kwargs = kwargs)
  val jplTropicsAbovegroundBiomassDensity2000: JplTropicsAbovegroundBiomassDensity2000 =
    JplTropicsAbovegroundBiomassDensity2000(gridTile, kwargs)
  val stdevAnnualAbovegroundRemovalsCarbon: StdevAnnualAbovegroundRemovalsCarbon =
    StdevAnnualAbovegroundRemovalsCarbon(gridTile, kwargs = kwargs)
  val stdevSoilCarbon2000: StdevSoilCarbon2000 = StdevSoilCarbon2000(gridTile, kwargs = kwargs)

  val fluxModelExtent: FluxModelExtent = FluxModelExtent(gridTile, kwargs = kwargs)
  val removalForestType: RemovalForestType = RemovalForestType(gridTile, kwargs = kwargs)
  val treeCoverGain: TreeCoverGain = TreeCoverGain(gridTile, kwargs)
  val mangroveBiomassExtent: MangroveBiomassExtent = MangroveBiomassExtent(gridTile, kwargs)
  val treeCoverLossDrivers: TreeCoverLossDrivers = TreeCoverLossDrivers(gridTile, kwargs)
  val faoEcozones: FaoEcozones = FaoEcozones(gridTile, kwargs = kwargs)
  val protectedAreas: ProtectedAreas = ProtectedAreas(gridTile, kwargs)
  val landmark: Landmark = Landmark(gridTile, kwargs)
  val intactForestLandscapes: IntactForestLandscapes = IntactForestLandscapes(gridTile, kwargs)
  val plantationsTypeFluxModel: PlantationsTypeFluxModel = PlantationsTypeFluxModel(gridTile, kwargs)
  val intactPrimaryForest: IntactPrimaryForest = IntactPrimaryForest(gridTile, kwargs)
  val peatlandsExtentFluxModel: PeatlandsExtentFluxModel = PeatlandsExtentFluxModel(gridTile, kwargs)
  val forestAgeCategory: ForestAgeCategory = ForestAgeCategory(gridTile, kwargs = kwargs)
  val jplTropicsAbovegroundBiomassExtent2000: JplTropicsAbovegroundBiomassExtent2000 =
    JplTropicsAbovegroundBiomassExtent2000(gridTile, kwargs)
  val fiaRegionsUsExtent: FiaRegionsUsExtent = FiaRegionsUsExtent(gridTile, kwargs)
  val brazilBiomes: BrazilBiomes = BrazilBiomes(gridTile, kwargs)
  val riverBasins: RiverBasins = RiverBasins(gridTile, kwargs)
  val primaryForest: PrimaryForest = PrimaryForest(gridTile, kwargs)
  val treeCoverLossLegalAmazon: TreeCoverLossLegalAmazon = TreeCoverLossLegalAmazon(gridTile, kwargs)
  val prodesLegalAmazonExtent2000: ProdesLegalAmazonExtent2000 =
    ProdesLegalAmazonExtent2000(gridTile, kwargs)
  val tropicLatitudeExtent: TropicLatitudeExtent = TropicLatitudeExtent(gridTile, kwargs)
  val burnYearHansenLoss: BurnYearHansenLoss = BurnYearHansenLoss(gridTile, kwargs)
  val grossEmissionsNodeCodes: GrossEmissionsNodeCodes =
    GrossEmissionsNodeCodes(gridTile, kwargs = kwargs)

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
      val grossAnnualAbovegroundRemovalsCarbonTile =
        grossAnnualAbovegroundRemovalsCarbon.fetchWindow(
          windowKey,
          windowLayout
        )
      val grossAnnualBelowgroundRemovalsCarbonTile =
        grossAnnualBelowgroundRemovalsCarbon.fetchWindow(
          windowKey,
          windowLayout
        )
      val grossCumulAbovegroundRemovalsCo2Tile =
        grossCumulAbovegroundRemovalsCo2.fetchWindow(windowKey, windowLayout)
      val grossCumulBelowgroundRemovalsCo2Tile =
        grossCumulBelowgroundRemovalsCo2.fetchWindow(windowKey, windowLayout)
      val netFluxCo2Tile = netFluxCo2.fetchWindow(windowKey, windowLayout)
      val agcEmisYearTile = agcEmisYear.fetchWindow(windowKey, windowLayout)
      val bgcEmisYearTile = bgcEmisYear.fetchWindow(windowKey, windowLayout)
      val deadwoodCarbonEmisYearTile =
        deadwoodCarbonEmisYear.fetchWindow(windowKey, windowLayout)
      val litterCarbonEmisYearTile =
        litterCarbonEmisYear.fetchWindow(windowKey, windowLayout)
      val soilCarbonEmisYearTile =
        soilCarbonEmisYear.fetchWindow(windowKey, windowLayout)
      val agc2000Tile = agc2000.fetchWindow(windowKey, windowLayout)
      val bgc2000Tile = bgc2000.fetchWindow(windowKey, windowLayout)
      val deadwoodCarbon2000Tile =
        deadwoodCarbon2000.fetchWindow(windowKey, windowLayout)
      val litterCarbon2000Tile =
        litterCarbon2000.fetchWindow(windowKey, windowLayout)
      val soilCarbon2000Tile =
        soilCarbon2000.fetchWindow(windowKey, windowLayout)
      val grossEmissionsCo2eNonCo2BiomassSoilTile =
        grossEmissionsCo2eNonCo2BiomassSoil.fetchWindow(windowKey, windowLayout)
      val grossEmissionsCo2eCo2OnlyBiomassSoilTile =
        grossEmissionsCo2eCo2OnlyBiomassSoil.fetchWindow(windowKey, windowLayout)
      val grossEmissionsCo2eNonCo2SoilOnlyTile =
        grossEmissionsCo2eNonCo2SoilOnly.fetchWindow(windowKey, windowLayout)
      val grossEmissionsCo2eCo2OnlySoilOnlyTile =
        grossEmissionsCo2eCo2OnlySoilOnly.fetchWindow(windowKey, windowLayout)
      val jplTropicsAbovegroundBiomassDensity2000Tile =
        jplTropicsAbovegroundBiomassDensity2000.fetchWindow(
          windowKey,
          windowLayout
        )
      val stdevAnnualAbovegroundRemovalsCarbonTile =
        stdevAnnualAbovegroundRemovalsCarbon.fetchWindow(
          windowKey,
          windowLayout
        )
      val stdevSoilCarbon2000Tile =
        stdevSoilCarbon2000.fetchWindow(windowKey, windowLayout)

      val fluxModelExtentTile =
        fluxModelExtent.fetchWindow(windowKey, windowLayout)
      val removalForestTypeTile =
        removalForestType.fetchWindow(windowKey, windowLayout)
      val mangroveBiomassExtentTile =
        mangroveBiomassExtent.fetchWindow(windowKey, windowLayout)
      val driversTile =
        treeCoverLossDrivers.fetchWindow(windowKey, windowLayout)
      val faoEcozonesTile = faoEcozones.fetchWindow(windowKey, windowLayout)
      val landmarkTile = landmark.fetchWindow(windowKey, windowLayout)
      val wdpaTile = protectedAreas.fetchWindow(windowKey, windowLayout)
      val intactForestLandscapesTile =
        intactForestLandscapes.fetchWindow(windowKey, windowLayout)
      val plantationsTypeFluxTile =
        plantationsTypeFluxModel.fetchWindow(windowKey, windowLayout)
      val intactPrimaryForestTile =
        intactPrimaryForest.fetchWindow(windowKey, windowLayout)
      val peatlandsExtentFluxTile =
        peatlandsExtentFluxModel.fetchWindow(windowKey, windowLayout)
      val forestAgeCategoryTile =
        forestAgeCategory.fetchWindow(windowKey, windowLayout)
      val jplTropicsAbovegroundBiomassExtent2000Tile =
        jplTropicsAbovegroundBiomassExtent2000.fetchWindow(
          windowKey,
          windowLayout
        )
      val fiaRegionsUsExtentTile =
        fiaRegionsUsExtent.fetchWindow(windowKey, windowLayout)
      val braBiomesTile = brazilBiomes.fetchWindow(windowKey, windowLayout)
      val riverBasinsTile = riverBasins.fetchWindow(windowKey, windowLayout)
      val primaryForestTile = primaryForest.fetchWindow(windowKey, windowLayout)
      val treeCoverLossLegalAmazonTile =
        treeCoverLossLegalAmazon.fetchWindow(windowKey, windowLayout)
      val prodesLegalAmazonExtent2000Tile =
        prodesLegalAmazonExtent2000.fetchWindow(windowKey, windowLayout)
      val tropicLatitudeExtentTile =
        tropicLatitudeExtent.fetchWindow(windowKey, windowLayout)
      val burnYearHansenLossTile =
        burnYearHansenLoss.fetchWindow(windowKey, windowLayout)
      val grossEmissionsNodeCodesTile =
        grossEmissionsNodeCodes.fetchWindow(windowKey, windowLayout)

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
        agc2000Tile,
        bgc2000Tile,
        deadwoodCarbon2000Tile,
        litterCarbon2000Tile,
        soilCarbon2000Tile,
        grossEmissionsCo2eNonCo2BiomassSoilTile,
        grossEmissionsCo2eCo2OnlyBiomassSoilTile,
        grossEmissionsCo2eNonCo2SoilOnlyTile,
        grossEmissionsCo2eCo2OnlySoilOnlyTile,
        jplTropicsAbovegroundBiomassDensity2000Tile,
        stdevAnnualAbovegroundRemovalsCarbonTile,
        stdevSoilCarbon2000Tile,
        fluxModelExtentTile,
        removalForestTypeTile,
        mangroveBiomassExtentTile,
        driversTile,
        faoEcozonesTile,
        landmarkTile,
        wdpaTile,
        intactForestLandscapesTile,
        plantationsTypeFluxTile,
        intactPrimaryForestTile,
        peatlandsExtentFluxTile,
        forestAgeCategoryTile,
        jplTropicsAbovegroundBiomassExtent2000Tile,
        fiaRegionsUsExtentTile,
        braBiomesTile,
        riverBasinsTile,
        primaryForestTile,
        treeCoverLossLegalAmazonTile,
        prodesLegalAmazonExtent2000Tile,
        tropicLatitudeExtentTile,
        burnYearHansenLossTile,
        grossEmissionsNodeCodesTile
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
