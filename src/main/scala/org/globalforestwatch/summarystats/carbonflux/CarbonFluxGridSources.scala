package org.globalforestwatch.summarystats.carbonflux

import cats.implicits._
import geotrellis.raster.Raster
import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridSources, GridTile}
import org.globalforestwatch.layers.{TropicLatitudeExtent, _}

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

  val stdevAnnualAbovegroundRemovalsCarbon = StdevAnnualAbovegroundRemovalsCarbon(gridTile)
  val stdevSoilCarbon2000 = StdevSoilCarbon2000(gridTile)

  val fluxModelExtent = FluxModelExtent(gridTile)
  val removalForestType = RemovalForestType(gridTile)
  val treeCoverGain = TreeCoverGain(gridTile)
  val mangroveBiomassExtent = MangroveBiomassExtent(gridTile)
  val treeCoverLossDrivers = TreeCoverLossDrivers(gridTile)
  val ecozones = Ecozones(gridTile)
  val protectedAreas = ProtectedAreas(gridTile)
  val landRights = LandRights(gridTile)
  val intactForestLandscapes = IntactForestLandscapes(gridTile)
  val plantationsTypeFluxModel = PlantationsTypeFluxModel(gridTile)
  val intactPrimaryForest = IntactPrimaryForest(gridTile)
  val peatlandsExtentFluxModel = PeatlandsExtentFluxModel(gridTile)
  val forestAgeCategory = ForestAgeCategory(gridTile)
  val jplTropicsAbovegroundBiomassExtent2000 = JplTropicsAbovegroundBiomassExtent2000(gridTile)
  val fiaRegionsUsExtent = FiaRegionsUsExtent(gridTile)
  val jplTropicsAbovegroundBiomassDensity2000 = JplTropicsAbovegroundBiomassDensity2000(gridTile)
  val brazilBiomes = BrazilBiomes(gridTile)
  val riverBasins = RiverBasins(gridTile)
  val primaryForest = PrimaryForest(gridTile)
  val treeCoverLossLegalAmazon = TreeCoverLossLegalAmazon(gridTile)
  val prodesLegalAmazonExtent2000 = ProdesLegalAmazonExtent2000(gridTile)
  val tropicLatitudeExtent = TropicLatitudeExtent(gridTile)
  val burnYearHansenLoss = BurnYearHansenLoss(gridTile)
  val grossEmissionsNodeCodes = GrossEmissionsNodeCodes(gridTile)

  def readWindow(window: Extent): Either[Throwable, Raster[CarbonFluxTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      lossTile <- Either.catchNonFatal(treeCoverLoss.fetchWindow(window)).right
      gainTile <- Either.catchNonFatal(treeCoverGain.fetchWindow(window)).right
      tcd2000Tile <- Either
        .catchNonFatal(treeCoverDensity2000.fetchWindow(window))
        .right

    } yield {
      // Failure for these will be converted to optional result and propagated with TreeLossTile
      val biomassTile = biomassPerHectar.fetchWindow(window)
      val grossAnnualAbovegroundRemovalsCarbonTile = grossAnnualAbovegroundRemovalsCarbon.fetchWindow(window)
      val grossAnnualBelowgroundRemovalsCarbonTile = grossAnnualBelowgroundRemovalsCarbon.fetchWindow(window)
      val grossCumulAbovegroundRemovalsCo2Tile = grossCumulAbovegroundRemovalsCo2.fetchWindow(window)
      val grossCumulBelowgroundRemovalsCo2Tile = grossCumulBelowgroundRemovalsCo2.fetchWindow(window)
      val netFluxCo2Tile = netFluxCo2.fetchWindow(window)
      val agcEmisYearTile = agcEmisYear.fetchWindow(window)
      val bgcEmisYearTile = bgcEmisYear.fetchWindow(window)
      val deadwoodCarbonEmisYearTile = deadwoodCarbonEmisYear.fetchWindow(window)
      val litterCarbonEmisYearTile = litterCarbonEmisYear.fetchWindow(window)
      val soilCarbonEmisYearTile = soilCarbonEmisYear.fetchWindow(window)
      val agc2000Tile = agc2000.fetchWindow(window)
      val bgc2000Tile = bgc2000.fetchWindow(window)
      val deadwoodCarbon2000Tile = deadwoodCarbon2000.fetchWindow(window)
      val litterCarbon2000Tile = litterCarbon2000.fetchWindow(window)
      val soilCarbon2000Tile = soilCarbon2000.fetchWindow(window)
      val grossEmissionsCo2eNonCo2Tile = grossEmissionsCo2eNonCo2.fetchWindow(window)
      val grossEmissionsCo2eCo2OnlyTile = grossEmissionsCo2eCo2Only.fetchWindow(window)
      val jplTropicsAbovegroundBiomassDensity2000Tile = jplTropicsAbovegroundBiomassDensity2000.fetchWindow(window)
      val stdevAnnualAbovegroundRemovalsCarbonTile = stdevAnnualAbovegroundRemovalsCarbon.fetchWindow(window)
      val stdevSoilCarbon2000Tile = stdevSoilCarbon2000.fetchWindow(window)

      val fluxModelExtentTile = fluxModelExtent.fetchWindow(window)
      val removalForestTypeTile = removalForestType.fetchWindow(window)
      val mangroveBiomassExtentTile = mangroveBiomassExtent.fetchWindow(window)
      val driversTile = treeCoverLossDrivers.fetchWindow(window)
      val ecozonesTile = ecozones.fetchWindow(window)
      val landRightsTile = landRights.fetchWindow(window)
      val wdpaTile = protectedAreas.fetchWindow(window)
      val intactForestLandscapesTile = intactForestLandscapes.fetchWindow(window)
      val plantationsTypeFluxTile = plantationsTypeFluxModel.fetchWindow(window)
      val intactPrimaryForestTile = intactPrimaryForest.fetchWindow(window)
      val peatlandsExtentFluxTile = peatlandsExtentFluxModel.fetchWindow(window)
      val forestAgeCategoryTile = forestAgeCategory.fetchWindow(window)
      val jplTropicsAbovegroundBiomassExtent2000Tile = jplTropicsAbovegroundBiomassExtent2000.fetchWindow(window)
      val fiaRegionsUsExtentTile = fiaRegionsUsExtent.fetchWindow(window)
      val braBiomesTile = brazilBiomes.fetchWindow(window)
      val riverBasinsTile = riverBasins.fetchWindow(window)
      val primaryForestTile = primaryForest.fetchWindow(window)
      val treeCoverLossLegalAmazonTile = treeCoverLossLegalAmazon.fetchWindow(window)
      val prodesLegalAmazonExtent2000Tile = prodesLegalAmazonExtent2000.fetchWindow(window)
      val tropicLatitudeExtentTile = tropicLatitudeExtent.fetchWindow(window)
      val burnYearHansenLossTile = burnYearHansenLoss.fetchWindow(window)
      val grossEmissionsNodeCodesTile = grossEmissionsNodeCodes.fetchWindow(window)

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
        landRightsTile,
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

      Raster(tile, window)
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
