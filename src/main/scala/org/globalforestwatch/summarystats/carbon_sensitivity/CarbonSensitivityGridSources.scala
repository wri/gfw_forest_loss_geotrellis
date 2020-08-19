package org.globalforestwatch.summarystats.carbon_sensitivity

import cats.implicits._
import geotrellis.raster.Raster
import geotrellis.vector.Extent
import org.globalforestwatch.grids.{GridSources, GridTile}
import org.globalforestwatch.layers._
import org.globalforestwatch.util.Util.getAnyMapValue

/**
  * @param gridTile top left corner, padded from east ex: "10N_010E"
  */
case class CarbonSensitivityGridSources(gridTile: GridTile, kwargs:  Map[String, Any]) extends GridSources {

  val model: String = getAnyMapValue[String](kwargs,"sensitivityType")

  val treeCoverLoss = TreeCoverLoss(gridTile)
  val treeCoverDensity2000 = TreeCoverDensityPercent2000(gridTile)

  val biomassPerHectar = BiomassPerHectar(gridTile)
//  val grossAnnualRemovalsCarbon = GrossAnnualRemovalsBiomass(gridId)
  val grossCumulRemovalsCarbon = GrossCumulRemovalsCo2(gridTile, model)
  val netFluxCo2 = NetFluxCo2e(gridTile, model)

  val agcEmisYear = AgcEmisYear(gridTile, model)
//  val bgcEmisYear = BgcEmisYear(gridId)
//  val deadwoodCarbonEmisYear = DeadwoodCarbonEmisYear(gridId)
//  val litterCarbonEmisYear = LitterCarbonEmisYear(gridId)
  val soilCarbonEmisYear = SoilCarbonEmisYear(gridTile, model)
//  val totalCarbonEmisYear = TotalCarbonEmisYear(gridId)

  val agc2000 = Agc2000(gridTile, model)
//  val bgc2000 = Bgc2000(gridId)
//  val deadwoodCarbon2000 = DeadwoodCarbon2000(gridId)
//  val litterCarbon2000 = LitterCarbon2000(gridId)
  val soilCarbon2000 = SoilCarbon2000(gridTile, model)
//  val totalCarbon2000 = TotalCarbon2000(gridId)

  val grossEmissionsCo2eNoneCo2 = GrossEmissionsNonCo2Co2e(gridTile, model)
  val grossEmissionsCo2eCo2Only = GrossEmissionsCo2OnlyCo2e(gridTile, model)

  val treeCoverGain = TreeCoverGain(gridTile)
  val mangroveBiomassExtent = MangroveBiomassExtent(gridTile)
  val treeCoverLossDrivers = TreeCoverLossDrivers(gridTile)
  val ecozones = Ecozones(gridTile)
  val protectedAreas = ProtectedAreas(gridTile)
  val landRights = LandRights(gridTile)
  val intactForestLandscapes = IntactForestLandscapes(gridTile)
  val plantations = Plantations(gridTile)
  val intactPrimaryForest = IntactPrimaryForest(gridTile)
  val peatlandsFlux = PeatlandsFlux(gridTile)
  val forestAgeCategory = ForestAgeCategory(gridTile)
  val jplTropicsAbovegroundBiomassExtent2000 = JplTropicsAbovegroundBiomassExtent2000(gridTile)
  val fiaRegionsUsExtent = FiaRegionsUsExtent(gridTile)
  val brazilBiomes = BrazilBiomes(gridTile)
  val riverBasins = RiverBasins(gridTile)
  val primaryForest = PrimaryForest(gridTile)
  val treeCoverLossLegalAmazon = TreeCoverLossLegalAmazon(gridTile)
  val prodesLegalAmazonExtent2000 = ProdesLegalAmazonExtent2000(gridTile)
  val treeCoverLossFirstYear20012015Mekong = TreeCoverLossFirstYear20012015Mekong(gridTile)
  val mekongTreeCoverLossExtent = MekongTreeCoverLossExtent(gridTile)
  val tropicLatitudeExtent = TropicLatitudeExtent(gridTile)

  val jplTropicsAbovegroundBiomassDensity2000 = JplTropicsAbovegroundBiomassDensity2000(gridTile)

  def readWindow(window: Extent): Either[Throwable, Raster[CarbonSensitivityTile]] = {

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
      //      val grossAnnualRemovalsCarbonTile =
      //        grossAnnualRemovalsCarbon.fetchWindow(window)
      val grossCumulRemovalsCarbonTile =
      grossCumulRemovalsCarbon.fetchWindow(window)
      val netFluxCo2Tile = netFluxCo2.fetchWindow(window)
      val agcEmisYearTile = agcEmisYear.fetchWindow(window)
      //      val bgcEmisYearTile = bgcEmisYear.fetchWindow(window)
      //      val deadwoodCarbonEmisYearTile =
      //        deadwoodCarbonEmisYear.fetchWindow(window)
      //      val litterCarbonEmisYearTile = litterCarbonEmisYear.fetchWindow(window)
      val soilCarbonEmisYearTile = soilCarbonEmisYear.fetchWindow(window)
      //      val totalCarbonEmisYearTile = totalCarbonEmisYear.fetchWindow(window)
      val agc2000Tile = agc2000.fetchWindow(window)
      //      val bgc2000Tile = bgc2000.fetchWindow(window)
      //      val deadwoodCarbon2000Tile = deadwoodCarbon2000.fetchWindow(window)
      //      val litterCarbon2000Tile = litterCarbon2000.fetchWindow(window)
      val soilCarbon2000Tile = soilCarbon2000.fetchWindow(window)
      //      val totalCarbon2000Tile = totalCarbon2000.fetchWindow(window)
      val grossEmissionsCo2eNoneCo2Tile =
        grossEmissionsCo2eNoneCo2.fetchWindow(window)
      val grossEmissionsCo2eCo2OnlyTile =
        grossEmissionsCo2eCo2Only.fetchWindow(window)
      val jplTropicsAbovegroundBiomassDensity2000Tile =
        jplTropicsAbovegroundBiomassDensity2000.fetchWindow(window)

      val mangroveBiomassExtentTile = mangroveBiomassExtent.fetchWindow(window)
      val driversTile = treeCoverLossDrivers.fetchWindow(window)
      val ecozonesTile = ecozones.fetchWindow(window)
      val landRightsTile = landRights.fetchWindow(window)
      val wdpaTile = protectedAreas.fetchWindow(window)
      val intactForestLandscapesTile =
        intactForestLandscapes.fetchWindow(window)
      val plantationsTile = plantations.fetchWindow(window)
      val intactPrimaryForestTile = intactPrimaryForest.fetchWindow(window)
      val peatlandFluxTile = peatlandsFlux.fetchWindow(window)
      val forestAgeCategoryTile = forestAgeCategory.fetchWindow(window)
      val jplTropicsAbovegroundBiomassExtent2000Tile = jplTropicsAbovegroundBiomassExtent2000.fetchWindow(window)
      val fiaRegionsUsExtentTile = fiaRegionsUsExtent.fetchWindow(window)
      val braBiomesTile = brazilBiomes.fetchWindow(window)
      val riverBasinsTile = riverBasins.fetchWindow(window)
      val primaryForestTile = primaryForest.fetchWindow(window)
      val treeCoverLossLegalAmazonTile = treeCoverLossLegalAmazon.fetchWindow(window)
      val prodesLegalAmazonExtent2000Tile = prodesLegalAmazonExtent2000.fetchWindow(window)
      val treeCoverLossFirstYear20012015MekongTile = treeCoverLossFirstYear20012015Mekong.fetchWindow(window)
      val mekongTreeCoverLossExtentTile = mekongTreeCoverLossExtent.fetchWindow(window)
      val tropicLatitudeExtentTile = tropicLatitudeExtent.fetchWindow(window)

      val tile = CarbonSensitivityTile(
        lossTile,
        gainTile,
        tcd2000Tile,
        biomassTile,
        //        grossAnnualRemovalsCarbonTile,
        grossCumulRemovalsCarbonTile,
        netFluxCo2Tile,
        agcEmisYearTile,
        //        bgcEmisYearTile,
        //        deadwoodCarbonEmisYearTile,
        //        litterCarbonEmisYearTile,
        soilCarbonEmisYearTile,
        //        totalCarbonEmisYearTile,
        agc2000Tile,
        //        bgc2000Tile,
        //        deadwoodCarbon2000Tile,
        //        litterCarbon2000Tile,
        soilCarbon2000Tile,
        //        totalCarbon2000Tile,
        grossEmissionsCo2eNoneCo2Tile,
        grossEmissionsCo2eCo2OnlyTile,
        jplTropicsAbovegroundBiomassDensity2000Tile,
        mangroveBiomassExtentTile,
        driversTile,
        ecozonesTile,
        landRightsTile,
        wdpaTile,
        intactForestLandscapesTile,
        plantationsTile,
        intactPrimaryForestTile,
        peatlandFluxTile,
        forestAgeCategoryTile,
        jplTropicsAbovegroundBiomassExtent2000Tile,
        fiaRegionsUsExtentTile,
        braBiomesTile,
        riverBasinsTile,
        primaryForestTile,
        treeCoverLossLegalAmazonTile,
        prodesLegalAmazonExtent2000Tile,
        treeCoverLossFirstYear20012015MekongTile,
        mekongTreeCoverLossExtentTile,
        tropicLatitudeExtentTile
      )

      Raster(tile, window)
    }
  }
}

object CarbonSensitivityGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap.empty[String, CarbonSensitivityGridSources]

  def getCachedSources(gridTile: GridTile, kwargs:  Map[String, Any]): CarbonSensitivityGridSources = {

    cache.getOrElseUpdate(gridTile.tileId, CarbonSensitivityGridSources(gridTile: GridTile, kwargs:  Map[String, Any]))

  }

}