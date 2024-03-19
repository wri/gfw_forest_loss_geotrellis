package org.globalforestwatch.summarystats.afi

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.Raster
import org.globalforestwatch.grids.{GridSources, GridTile}
import org.globalforestwatch.layers._

/**
  * @param gridTile top left corner, padded from east ex: "10N_010E"
  */
case class AFiGridSources(gridTile: GridTile, kwargs: Map[String, Any]) extends GridSources {
  val treeCoverLoss: TreeCoverLoss = TreeCoverLoss(gridTile, kwargs)
  val sbtnNaturalForest: SBTNNaturalForests = SBTNNaturalForests(gridTile, kwargs)
  val negligibleRisk: NegligibleRisk = NegligibleRisk(gridTile, kwargs)
  val gadmAdm0: GadmAdm0 = GadmAdm0(gridTile, kwargs)
  val gadmAdm1: GadmAdm1 = GadmAdm1(gridTile, kwargs)
  val gadmAdm2: GadmAdm2 = GadmAdm2(gridTile, kwargs)
  val jrcForestCover: JRCForestCover = JRCForestCover(gridTile, kwargs)

  def readWindow(
    windowKey: SpatialKey,
    windowLayout: LayoutDefinition
  ): Either[Throwable, Raster[AFiTile]] = {
    for {
      lossTile <- Either.catchNonFatal(treeCoverLoss.fetchWindow(windowKey, windowLayout)).right
    } yield {

      val sbtnNaturalForestTile = sbtnNaturalForest.fetchWindow(windowKey, windowLayout)
      val negligibleRiskTile = negligibleRisk.fetchWindow(windowKey, windowLayout)
      val adm0Tile = gadmAdm0.fetchWindow(windowKey, windowLayout)
      val adm1Tile = gadmAdm1.fetchWindow(windowKey, windowLayout)
      val adm2Tile = gadmAdm2.fetchWindow(windowKey, windowLayout)
      val jrcForestCoverTile = jrcForestCover.fetchWindow(windowKey, windowLayout)

      val tile = AFiTile(
        lossTile,
        sbtnNaturalForestTile,
        negligibleRiskTile,
        adm0Tile,
        adm1Tile,
        adm2Tile,
        jrcForestCoverTile
      )
      Raster(tile, windowKey.extent(windowLayout))
    }
  }
}

object AFiGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap
      .empty[String, AFiGridSources]

  def getCachedSources(gridTile: GridTile, kwargs: Map[String, Any]): AFiGridSources = {
    cache.getOrElseUpdate(gridTile.tileId, AFiGridSources(gridTile, kwargs))
  }
}
