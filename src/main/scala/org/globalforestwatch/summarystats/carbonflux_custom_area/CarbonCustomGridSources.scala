package org.globalforestwatch.summarystats.carbonflux_custom_area

import cats.implicits._
import geotrellis.raster.Raster
import geotrellis.vector.Extent
import org.globalforestwatch.grids.GridSources
import org.globalforestwatch.layers._

/**
  * @param gridId top left corner, padded from east ex: "10N_010E"
  */
case class CarbonCustomGridSources(gridId: String) extends GridSources {

  val treeCoverLoss = TreeCoverLoss(gridId)
  val treeCoverDensity2000 = TreeCoverDensityThresholds2000(gridId)

  val biomassPerHectar = BiomassPerHectar(gridId)
  val grossAnnualRemovalsCarbon = GrossAnnualRemovalsBiomass(gridId)
  val grossCumulRemovalsCarbon = GrossCumulRemovalsCo2(gridId)
  val netFluxCo2 = NetFluxCo2e(gridId)

  val grossEmissionsCo2eNoneCo2 = GrossEmissionsNonCo2Co2e(gridId)
  val grossEmissionsCo2eCo2Only = GrossEmissionsCo2OnlyCo2e(gridId)


  val treeCoverGain = TreeCoverGain(gridId)
  val carbonFluxCustomArea1 = CarbonFluxCustomArea1(gridId)

  def readWindow(window: Extent): Either[Throwable, Raster[CarbonCustomTile]] = {

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
      val grossEmissionsCo2eNoneCo2Tile =
        grossEmissionsCo2eNoneCo2.fetchWindow(window)
      val grossEmissionsCo2eCo2OnlyTile =
        grossEmissionsCo2eCo2Only.fetchWindow(window)

      val carbonFluxCustomArea1Tile = carbonFluxCustomArea1.fetchWindow(window)

      val tile = CarbonCustomTile(
        lossTile,
        gainTile,
        tcd2000Tile,
        biomassTile,
        grossAnnualRemovalsCarbonTile,
        grossCumulRemovalsCarbonTile,
        netFluxCo2Tile,
        grossEmissionsCo2eNoneCo2Tile,
        grossEmissionsCo2eCo2OnlyTile,
        carbonFluxCustomArea1Tile
      )

      Raster(tile, window)
    }
  }
}

object CarbonCustomGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap.empty[String, CarbonCustomGridSources]

  def getCachedSources(gridId: String): CarbonCustomGridSources = {

    cache.getOrElseUpdate(gridId, CarbonCustomGridSources(gridId))

  }

}
