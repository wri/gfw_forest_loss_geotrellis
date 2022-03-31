package org.globalforestwatch.summarystats.carbon_sensitivity

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.Raster
import org.globalforestwatch.grids.{GridSources, GridTile}
import org.globalforestwatch.layers._
import org.globalforestwatch.util.Util.getAnyMapValue

/**
  * @param gridTile top left corner, padded from east ex: "10N_010E"
  */
case class CarbonSensitivityGridSources(gridTile: GridTile,
                                        kwargs: Map[String, Any])
  extends GridSources {

  val model: String = getAnyMapValue[String](kwargs, "sensitivityType")

  val treeCoverLoss: TreeCoverLoss = TreeCoverLoss(gridTile, kwargs)
  val treeCoverDensity2000: TreeCoverDensityThreshold2000 = TreeCoverDensityThreshold2000(gridTile, kwargs)
  val biomassPerHectar: BiomassPerHectar = BiomassPerHectar(gridTile, kwargs)
  val grossCumulAbovegroundRemovalsCo2: GrossCumulAbovegroundRemovalsCo2 =
    GrossCumulAbovegroundRemovalsCo2(gridTile, model, kwargs)
  val grossCumulBelowgroundRemovalsCo2: GrossCumulBelowgroundRemovalsCo2 =
    GrossCumulBelowgroundRemovalsCo2(gridTile, model, kwargs)
  val netFluxCo2: NetFluxCo2e = NetFluxCo2e(gridTile, model, kwargs)
  val agcEmisYear: AgcEmisYear = AgcEmisYear(gridTile, model, kwargs)
  val soilCarbonEmisYear: SoilCarbonEmisYear = SoilCarbonEmisYear(gridTile, model, kwargs)
  val grossEmissionsCo2eNonCo2: GrossEmissionsNonCo2Co2e =
    GrossEmissionsNonCo2Co2e(gridTile, model, kwargs)
  val grossEmissionsCo2eCo2Only: GrossEmissionsCo2OnlyCo2e =
    GrossEmissionsCo2OnlyCo2e(gridTile, model, kwargs)
  val jplTropicsAbovegroundBiomassDensity2000: JplTropicsAbovegroundBiomassDensity2000 =
    JplTropicsAbovegroundBiomassDensity2000(gridTile, kwargs)
  val fluxModelExtent: FluxModelExtent = FluxModelExtent(gridTile, model, kwargs)
  val removalForestType: RemovalForestType = RemovalForestType(gridTile, model, kwargs)
  val treeCoverGain: TreeCoverGain = TreeCoverGain(gridTile, kwargs)
  val mangroveBiomassExtent: MangroveBiomassExtent = MangroveBiomassExtent(gridTile, kwargs)
  val treeCoverLossDrivers: TreeCoverLossDrivers = TreeCoverLossDrivers(gridTile, kwargs)
  val ecozones: Ecozones = Ecozones(gridTile, kwargs)
  val protectedAreas: ProtectedAreas = ProtectedAreas(gridTile, kwargs)
  val landmark: Landmark = Landmark(gridTile, kwargs)
  val intactForestLandscapes: IntactForestLandscapes = IntactForestLandscapes(gridTile, kwargs)
  val plantationsTypeFluxModel: PlantationsTypeFluxModel = PlantationsTypeFluxModel(gridTile, kwargs)
  val intactPrimaryForest: IntactPrimaryForest = IntactPrimaryForest(gridTile, kwargs)
  val peatlandsExtentFluxModel: PeatlandsExtentFluxModel = PeatlandsExtentFluxModel(gridTile, kwargs)
  val forestAgeCategory: ForestAgeCategory = ForestAgeCategory(gridTile, model, kwargs)
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
  val grossEmissionsNodeCodes: GrossEmissionsNodeCodes = GrossEmissionsNodeCodes(gridTile, model, kwargs)

  def readWindow(
                  windowKey: SpatialKey,
                  windowLayout: LayoutDefinition
                ): Either[Throwable, Raster[CarbonSensitivityTile]] = {

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
      val grossCumulAbovegroundRemovalsCo2Tile =
        grossCumulAbovegroundRemovalsCo2.fetchWindow(windowKey, windowLayout)
      val grossCumulBelowgroundRemovalsCo2Tile =
        grossCumulBelowgroundRemovalsCo2.fetchWindow(windowKey, windowLayout)
      val netFluxCo2Tile = netFluxCo2.fetchWindow(windowKey, windowLayout)
      val agcEmisYearTile = agcEmisYear.fetchWindow(windowKey, windowLayout)
      val soilCarbonEmisYearTile =
        soilCarbonEmisYear.fetchWindow(windowKey, windowLayout)
      val grossEmissionsCo2eNonCo2Tile =
        grossEmissionsCo2eNonCo2.fetchWindow(windowKey, windowLayout)
      val grossEmissionsCo2eCo2OnlyTile =
        grossEmissionsCo2eCo2Only.fetchWindow(windowKey, windowLayout)
      val jplTropicsAbovegroundBiomassDensity2000Tile =
        jplTropicsAbovegroundBiomassDensity2000.fetchWindow(
          windowKey,
          windowLayout
        )

      val fluxModelExtentTile =
        fluxModelExtent.fetchWindow(windowKey, windowLayout)
      val removalForestTypeTile =
        removalForestType.fetchWindow(windowKey, windowLayout)
      val mangroveBiomassExtentTile =
        mangroveBiomassExtent.fetchWindow(windowKey, windowLayout)
      val driversTile =
        treeCoverLossDrivers.fetchWindow(windowKey, windowLayout)
      val ecozonesTile = ecozones.fetchWindow(windowKey, windowLayout)
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

      val tile = CarbonSensitivityTile(
        lossTile,
        gainTile,
        tcd2000Tile,
        biomassTile,
        grossCumulAbovegroundRemovalsCo2Tile,
        grossCumulBelowgroundRemovalsCo2Tile,
        netFluxCo2Tile,
        agcEmisYearTile,
        soilCarbonEmisYearTile,
        grossEmissionsCo2eNonCo2Tile,
        grossEmissionsCo2eCo2OnlyTile,
        jplTropicsAbovegroundBiomassDensity2000Tile,
        fluxModelExtentTile,
        removalForestTypeTile,
        mangroveBiomassExtentTile,
        driversTile,
        ecozonesTile,
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

object CarbonSensitivityGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap
      .empty[String, CarbonSensitivityGridSources]

  def getCachedSources(
                        gridTile: GridTile,
                        kwargs: Map[String, Any]
                      ): CarbonSensitivityGridSources = {

    cache.getOrElseUpdate(
      gridTile.tileId,
      CarbonSensitivityGridSources(gridTile: GridTile, kwargs: Map[String, Any])
    )

  }

}
