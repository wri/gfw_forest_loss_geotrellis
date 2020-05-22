package org.globalforestwatch.summarystats.carbonflux

import cats.implicits._
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
  val grossAnnualRemovalsCarbon = GrossAnnualRemovalsBiomass(gridTile)
  val grossCumulRemovalsCarbon = GrossCumulRemovalsCo2(gridTile)
  val netFluxCo2 = NetFluxCo2e(gridTile)

  val agcEmisYear = AgcEmisYear(gridTile)
  val bgcEmisYear = BgcEmisYear(gridTile)
  val deadwoodCarbonEmisYear = DeadwoodCarbonEmisYear(gridTile)
  val litterCarbonEmisYear = LitterCarbonEmisYear(gridTile)
  val soilCarbonEmisYear = SoilCarbonEmisYear(gridTile)
  val totalCarbonEmisYear = TotalCarbonEmisYear(gridTile)

  val agc2000 = Agc2000(gridTile)
  val bgc2000 = Bgc2000(gridTile)
  val deadwoodCarbon2000 = DeadwoodCarbon2000(gridTile)
  val litterCarbon2000 = LitterCarbon2000(gridTile)
  val soilCarbon2000 = SoilCarbon2000(gridTile)
  val totalCarbon2000 = TotalCarbon2000(gridTile)

  val grossEmissionsCo2eNoneCo2 = GrossEmissionsNonCo2Co2e(gridTile)
  val grossEmissionsCo2eCo2Only = GrossEmissionsCo2OnlyCo2e(gridTile)

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
  val jplAGBExtent = JplAGBextent(gridTile)
  val fiaRegionsUsExtent = FiaRegionsUsExtent(gridTile)

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
      val grossAnnualRemovalsCarbonTile =
        grossAnnualRemovalsCarbon.fetchWindow(window)
      val grossCumulRemovalsCarbonTile =
        grossCumulRemovalsCarbon.fetchWindow(window)
      val netFluxCo2Tile = netFluxCo2.fetchWindow(window)
      val agcEmisYearTile = agcEmisYear.fetchWindow(window)
      val bgcEmisYearTile = bgcEmisYear.fetchWindow(window)
      val deadwoodCarbonEmisYearTile =
        deadwoodCarbonEmisYear.fetchWindow(window)
      val litterCarbonEmisYearTile = litterCarbonEmisYear.fetchWindow(window)
      val soilCarbonEmisYearTile = soilCarbonEmisYear.fetchWindow(window)
      val totalCarbonEmisYearTile = totalCarbonEmisYear.fetchWindow(window)
      val agc2000Tile = agc2000.fetchWindow(window)
      val bgc2000Tile = bgc2000.fetchWindow(window)
      val deadwoodCarbon2000Tile = deadwoodCarbon2000.fetchWindow(window)
      val litterCarbon2000Tile = litterCarbon2000.fetchWindow(window)
      val soilCarbon2000Tile = soilCarbon2000.fetchWindow(window)
      val totalCarbon2000Tile = totalCarbon2000.fetchWindow(window)
      val grossEmissionsCo2eNoneCo2Tile =
        grossEmissionsCo2eNoneCo2.fetchWindow(window)
      val grossEmissionsCo2eCo2OnlyTile =
        grossEmissionsCo2eCo2Only.fetchWindow(window)

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
      val jplAGBextentTile = jplAGBExtent.fetchWindow(window)
      val fiaRegionsUsExtentTile = fiaRegionsUsExtent.fetchWindow(window)

      val tile = CarbonFluxTile(
        lossTile,
        gainTile,
        tcd2000Tile,
        biomassTile,
        grossAnnualRemovalsCarbonTile,
        grossCumulRemovalsCarbonTile,
        netFluxCo2Tile,
        agcEmisYearTile,
        bgcEmisYearTile,
        deadwoodCarbonEmisYearTile,
        litterCarbonEmisYearTile,
        soilCarbonEmisYearTile,
        totalCarbonEmisYearTile,
        agc2000Tile,
        bgc2000Tile,
        deadwoodCarbon2000Tile,
        litterCarbon2000Tile,
        soilCarbon2000Tile,
        totalCarbon2000Tile,
        grossEmissionsCo2eNoneCo2Tile,
        grossEmissionsCo2eCo2OnlyTile,
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
        jplAGBextentTile,
        fiaRegionsUsExtentTile
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
