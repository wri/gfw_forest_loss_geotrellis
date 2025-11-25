package org.globalforestwatch.summarystats.integrated_alerts

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.Raster
import org.globalforestwatch.grids.{GridSources, GridTile}
import org.globalforestwatch.layers._

/**
  * @param gridTile top left corner, padded from east ex: "10N_010E"
  */
case class IntegratedAlertsGridSources(gridTile: GridTile, kwargs: Map[String, Any]) extends GridSources {

  val gladAlertsL: GladAlerts = GladAlerts(gridTile, kwargs)
  val gladAlertsS2: GladAlertsS2 = GladAlertsS2(gridTile, kwargs)
  val raddAlerts: RaddAlerts = RaddAlerts(gridTile, kwargs)
  val distAlerts: DistAlerts = DistAlerts(gridTile, kwargs)
  val biomassPerHectar: AbovegroundBiomass2000 = AbovegroundBiomass2000(gridTile, kwargs)
  val primaryForest: PrimaryForest = PrimaryForest(gridTile, kwargs)
  val protectedAreas: ProtectedAreas = ProtectedAreas(gridTile, kwargs)
  val landmark: Landmark = Landmark(gridTile, kwargs)
  val peatlands: Peatlands = Peatlands(gridTile, kwargs)
  val mangroves2020: Mangroves2020 = Mangroves2020(gridTile, kwargs)
  val intactForestLandscapes2016: IntactForestLandscapes2016 = IntactForestLandscapes2016(gridTile, kwargs)
  val naturalForests: SBTNNaturalForests = SBTNNaturalForests(gridTile, kwargs)
  val treeCover2022: TreeCover2022 = TreeCover2022(gridTile, kwargs)

  def readWindow(windowKey: SpatialKey, windowLayout: LayoutDefinition): Either[Throwable, Raster[IntegratedAlertsTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      gladAlertsLTile <- Either
        .catchNonFatal(gladAlertsL.fetchWindow(windowKey, windowLayout))
        .right

    } yield {
      val gladAlertsS2Tile = gladAlertsS2.fetchWindow(windowKey, windowLayout)
      val raddAlertsTile = raddAlerts.fetchWindow(windowKey, windowLayout)
      val distAlertsTile = distAlerts.fetchWindow(windowKey, windowLayout)
      val biomassTile = biomassPerHectar.fetchWindow(windowKey, windowLayout)
      val primaryForestTile = primaryForest.fetchWindow(windowKey, windowLayout)
      val protectedAreasTile = protectedAreas.fetchWindow(windowKey, windowLayout)
      val landmarkTile = landmark.fetchWindow(windowKey, windowLayout)
      val peatlandsTile = peatlands.fetchWindow(windowKey, windowLayout)
      val mangroves2020Tile = mangroves2020.fetchWindow(windowKey, windowLayout)
      val intactForestLandscapes2016Tile = intactForestLandscapes2016.fetchWindow(windowKey, windowLayout)
      val naturalForestsTile = naturalForests.fetchWindow(windowKey, windowLayout)
      val treeCover2022Tile = treeCover2022.fetchWindow(windowKey, windowLayout)

      val tile = IntegratedAlertsTile(
        gladAlertsLTile,
        gladAlertsS2Tile,
        raddAlertsTile,
        distAlertsTile,
        biomassTile,
        primaryForestTile,
        protectedAreasTile,
        landmarkTile,
        peatlandsTile,
        mangroves2020Tile,
        intactForestLandscapes2016Tile,
        naturalForestsTile,
        treeCover2022Tile
      )

      Raster(tile, windowKey.extent(windowLayout))
    }
  }

}

object IntegratedAlertsGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap.empty[String, IntegratedAlertsGridSources]

  def getCachedSources(gridTile: GridTile, kwargs: Map[String, Any]): IntegratedAlertsGridSources = {

    cache.getOrElseUpdate(gridTile.tileId, IntegratedAlertsGridSources(gridTile, kwargs))

  }

}
