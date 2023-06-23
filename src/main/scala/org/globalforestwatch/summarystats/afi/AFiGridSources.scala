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

  def readWindow(
                  windowKey: SpatialKey,
                  windowLayout: LayoutDefinition
                ): Either[Throwable, Raster[AFiTile]] = {

    for {
      lossTile <- Either.catchNonFatal(treeCoverLoss.fetchWindow(windowKey, windowLayout)).right
    } yield {
      val tile = AFiTile(lossTile)
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
