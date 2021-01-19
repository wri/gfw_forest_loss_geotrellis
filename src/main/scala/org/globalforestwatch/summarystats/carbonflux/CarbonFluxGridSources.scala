package org.globalforestwatch.summarystats.carbonflux

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.Raster
import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridSources, GridTile}
import org.globalforestwatch.layers._

/**
  * @param gridTile top left corner, padded from east ex: "10N_010E"
  */
case class CarbonFluxGridSources(gridTile: GridTile) extends GridSources {

  val treeCoverLoss = TreeCoverLoss(gridTile)
  val treeCoverDensity2000 = TreeCoverDensityPercent2000(gridTile)
  val biomassPerHectar = BiomassPerHectar(gridTile)
  val grossAnnualAbovegroundRemovalsCarbon = GrossAnnualAbovegroundRemovalsCarbon(gridTile)
  val grossAnnualBelowgroundRemovalsCarbon = GrossAnnualBelowgroundRemovalsCarbon(gridTile)
  val grossCumulAbovegroundRemovalsCo2 = GrossCumulAbovegroundRemovalsCo2(gridTile)
  val grossCumulBelowgroundRemovalsCo2 = GrossCumulBelowgroundRemovalsCo2(gridTile)
  val netFluxCo2 = NetFluxCo2e(gridTile)
  val agcEmisYear = AgcEmisYear(gridTile)
  val bgcEmisYear = BgcEmisYear(gridTile)
  val deadwoodCarbonEmisYear = DeadwoodCarbonEmisYear(gridTile)
  val litterCarbonEmisYear = LitterCarbonEmisYear(gridTile)
  val soilCarbonEmisYear = SoilCarbonEmisYear(gridTile)
  val agc2000 = Agc2000(gridTile)
  val bgc2000 = Bgc2000(gridTile)
  val deadwoodCarbon2000 = DeadwoodCarbon2000(gridTile)
  val litterCarbon2000 = LitterCarbon2000(gridTile)
  val soilCarbon2000 = SoilCarbon2000(gridTile)
  val grossEmissionsCo2eNonCo2 = GrossEmissionsNonCo2Co2e(gridTile)
  val grossEmissionsCo2eCo2Only = GrossEmissionsCo2OnlyCo2e(gridTile)
  val jplTropicsAbovegroundBiomassDensity2000 = JplTropicsAbovegroundBiomassDensity2000(gridTile)
  val stdevAnnualAbovegroundRemovalsCarbon = StdevAnnualAbovegroundRemovalsCarbon(gridTile)
  val stdevSoilCarbon2000 = StdevSoilCarbon2000(gridTile)

  val fluxModelExtent = FluxModelExtent(gridTile)
  val removalForestType = RemovalForestType(gridTile)
  val treeCoverGain = TreeCoverGain(gridTile)
  val mangroveBiomassExtent = MangroveBiomassExtent(gridTile)
  val treeCoverLossDrivers = TreeCoverLossDrivers(gridTile)
  val ecozones = Ecozones(gridTile)
  val protectedAreas = ProtectedAreas(gridTile)
  val landmark = Landmark(gridTile)
  val intactForestLandscapes = IntactForestLandscapes(gridTile)
  val plantationsTypeFluxModel = PlantationsTypeFluxModel(gridTile)
  val intactPrimaryForest = IntactPrimaryForest(gridTile)
  val peatlandsExtentFluxModel = PeatlandsExtentFluxModel(gridTile)
  val forestAgeCategory = ForestAgeCategory(gridTile)
  val jplTropicsAbovegroundBiomassExtent2000 = JplTropicsAbovegroundBiomassExtent2000(gridTile)
  val fiaRegionsUsExtent = FiaRegionsUsExtent(gridTile)
  val brazilBiomes = BrazilBiomes(gridTile)
  val riverBasins = RiverBasins(gridTile)
  val primaryForest = PrimaryForest(gridTile)
  val treeCoverLossLegalAmazon = TreeCoverLossLegalAmazon(gridTile)
  val prodesLegalAmazonExtent2000 = ProdesLegalAmazonExtent2000(gridTile)
  val tropicLatitudeExtent = TropicLatitudeExtent(gridTile)
  val burnYearHansenLoss = BurnYearHansenLoss(gridTile)
  val grossEmissionsNodeCodes = GrossEmissionsNodeCodes(gridTile)

  def readWindow(
                  windowKey: SpatialKey, windowLayout: LayoutDefinition
                ): Either[Throwable, Raster[CarbonFluxTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      lossTile <- Either.catchNonFatal(treeCoverLoss.fetchWindow(windowKey, windowLayout)).right
      gainTile <- Either.catchNonFatal(treeCoverGain.fetchWindow(windowKey, windowLayout)).right
      tcd2000Tile <- Either
        .catchNonFatal(treeCoverDensity2000.fetchWindow(windowKey, windowLayout))
        .right

    } yield {
      // Failure for these will be converted to optional result and propagated with TreeLossTile
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
      val agc2000Tile = agc2000.fetchWindow(windowKey, windowLayout)
      val bgc2000Tile = bgc2000.fetchWindow(windowKey, windowLayout)
      val deadwoodCarbon2000Tile = deadwoodCarbon2000.fetchWindow(windowKey, windowLayout)
      val litterCarbon2000Tile = litterCarbon2000.fetchWindow(windowKey, windowLayout)
      val soilCarbon2000Tile = soilCarbon2000.fetchWindow(windowKey, windowLayout)
      val grossEmissionsCo2eNonCo2Tile = grossEmissionsCo2eNonCo2.fetchWindow(windowKey, windowLayout)
      val grossEmissionsCo2eCo2OnlyTile = grossEmissionsCo2eCo2Only.fetchWindow(windowKey, windowLayout)
      val jplTropicsAbovegroundBiomassDensity2000Tile = jplTropicsAbovegroundBiomassDensity2000.fetchWindow(windowKey, windowLayout)
      val stdevAnnualAbovegroundRemovalsCarbonTile = stdevAnnualAbovegroundRemovalsCarbon.fetchWindow(windowKey, windowLayout)
      val stdevSoilCarbon2000Tile = stdevSoilCarbon2000.fetchWindow(windowKey, windowLayout)

      val fluxModelExtentTile = fluxModelExtent.fetchWindow(windowKey, windowLayout)
      val removalForestTypeTile = removalForestType.fetchWindow(windowKey, windowLayout)
      val mangroveBiomassExtentTile = mangroveBiomassExtent.fetchWindow(windowKey, windowLayout)
      val driversTile = treeCoverLossDrivers.fetchWindow(windowKey, windowLayout)
      val ecozonesTile = ecozones.fetchWindow(windowKey, windowLayout)
      val landmarkTile = landmark.fetchWindow(windowKey, windowLayout)
      val wdpaTile = protectedAreas.fetchWindow(windowKey, windowLayout)
      val intactForestLandscapesTile = intactForestLandscapes.fetchWindow(windowKey, windowLayout)
      val plantationsTypeFluxTile = plantationsTypeFluxModel.fetchWindow(windowKey, windowLayout)
      val intactPrimaryForestTile = intactPrimaryForest.fetchWindow(windowKey, windowLayout)
      val peatlandsExtentFluxTile = peatlandsExtentFluxModel.fetchWindow(windowKey, windowLayout)
      val forestAgeCategoryTile = forestAgeCategory.fetchWindow(windowKey, windowLayout)
      val jplTropicsAbovegroundBiomassExtent2000Tile = jplTropicsAbovegroundBiomassExtent2000.fetchWindow(windowKey, windowLayout)
      val fiaRegionsUsExtentTile = fiaRegionsUsExtent.fetchWindow(windowKey, windowLayout)
      val braBiomesTile = brazilBiomes.fetchWindow(windowKey, windowLayout)
      val riverBasinsTile = riverBasins.fetchWindow(windowKey, windowLayout)
      val primaryForestTile = primaryForest.fetchWindow(windowKey, windowLayout)
      val treeCoverLossLegalAmazonTile = treeCoverLossLegalAmazon.fetchWindow(windowKey, windowLayout)
      val prodesLegalAmazonExtent2000Tile = prodesLegalAmazonExtent2000.fetchWindow(windowKey, windowLayout)
      val tropicLatitudeExtentTile = tropicLatitudeExtent.fetchWindow(windowKey, windowLayout)
      val burnYearHansenLossTile = burnYearHansenLoss.fetchWindow(windowKey, windowLayout)
      val grossEmissionsNodeCodesTile = grossEmissionsNodeCodes.fetchWindow(windowKey, windowLayout)

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
        grossEmissionsCo2eNonCo2Tile,
        grossEmissionsCo2eCo2OnlyTile,
        jplTropicsAbovegroundBiomassDensity2000Tile,
        stdevAnnualAbovegroundRemovalsCarbonTile,
        stdevSoilCarbon2000Tile,

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

object CarbonFluxGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap.empty[String, CarbonFluxGridSources]

  def getCachedSources(gridTile: GridTile): CarbonFluxGridSources = {

    cache.getOrElseUpdate(gridTile.tileId, CarbonFluxGridSources(gridTile))

  }

}
