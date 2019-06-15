package org.globalforestwatch.carbonflux

import geotrellis.raster.Raster
import geotrellis.vector.Extent
import cats.implicits._
import org.globalforestwatch.grids.GridSources
import org.globalforestwatch.layers._

/**
  * @param gridId top left corner, padded from east ex: "10N_010E"
  */
case class CarbonFluxGridSources(gridId: String) extends GridSources {

  val treeCoverLoss = TreeCoverLoss(gridId)
  val treeCoverDensity2000 = TreeCoverDensity2000(gridId)

  val biomassPerHectar = BiomassPerHectar(gridId)
  val grossAnnualRemovalsCarbon = GrossAnnualRemovalsCarbon(gridId)
  val grossCumulRemovalsCarbon = GrossCumulRemovalsCarbon(gridId)
  val netFluxCo2 = NetFluxCo2(gridId)

  val agcEmisYear = AgcEmisYear(gridId)
  val bgcEmisYear = BgcEmisYear(gridId)
  val deadwoodCarbonEmisYear = DeadwoodCarbonEmisYear(gridId)
  val litterCarbonEmisYear = LitterCarbonEmisYear(gridId)
  val soilCarbonEmisYear = SoilCarbonEmisYear(gridId)
  val totalCarbonEmisYear = TotalCarbonEmisYear(gridId)

  val agc2000 = Agc2000(gridId)
  val bgc2000 = Bgc2000(gridId)
  val deadwoodCarbon2000 = DeadwoodCarbon2000(gridId)
  val litterCarbon2000 = LitterCarbon2000(gridId)
  val soilCarbon2000 = SoilCarbon2000(gridId)
  val totalCarbon2000 = TotalCarbon2000(gridId)

  val grossEmissionsCo2 = GrossEmissionsCo2(gridId)

  val treeCoverGain = TreeCoverGain(gridId)
  val mangroveBiomassExtent = MangroveBiomassExtent(gridId)
  val treeCoverLossDrivers = TreeCoverLossDrivers(gridId)
  val ecozones = Ecozones(gridId)
  val protectedAreas = ProtectedAreas(gridId)
  val landRights = LandRights(gridId)
  val intactForestLandscapes = IntactForestLandscapes(gridId)
  val plantations = Plantations(gridId)

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
      val grossEmissionsCo2Tile = grossEmissionsCo2.fetchWindow(window)

      val mangroveBiomassExtentTile = mangroveBiomassExtent.fetchWindow(window)
      val driversTile = treeCoverLossDrivers.fetchWindow(window)
      val ecozonesTile = ecozones.fetchWindow(window)
      val landRightsTile = landRights.fetchWindow(window)
      val wdpaTile = protectedAreas.fetchWindow(window)
      val intactForestLandscapesTile =
        intactForestLandscapes.fetchWindow(window)
      val plantationsTile = plantations.fetchWindow(window)

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
        grossEmissionsCo2Tile,
        mangroveBiomassExtentTile,
        driversTile,
        ecozonesTile,
        landRightsTile,
        wdpaTile,
        intactForestLandscapesTile,
        plantationsTile
      )

      Raster(tile, window)
    }
  }
}

object CarbonFluxGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap.empty[String, CarbonFluxGridSources]

  def getCachedSources(gridId: String): CarbonFluxGridSources = {

    cache.getOrElseUpdate(gridId, CarbonFluxGridSources(gridId))

  }

}
