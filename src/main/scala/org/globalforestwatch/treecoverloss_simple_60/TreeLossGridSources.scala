package org.globalforestwatch.treecoverloss_simple_60

import cats.implicits._
import geotrellis.raster.Raster
import geotrellis.vector.Extent
import org.globalforestwatch.grids.GridSources
import org.globalforestwatch.layers._

/**
  * @param grid top left corner, padded from east ex: "10N_010E"
  */
case class TreeLossGridSources(grid: String) extends GridSources {

  lazy val treeCoverLoss = TreeCoverLoss(grid)
  lazy val treeCoverDensity2010_60 = TreeCoverDensity2010_60(grid)

  def readWindow(window: Extent): Either[Throwable, Raster[TreeLossTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      lossTile <- Either.catchNonFatal(treeCoverLoss.fetchWindow(window)).right
      tcd2010Tile <- Either
        .catchNonFatal(treeCoverDensity2010_60.fetchWindow(window))
        .right

    } yield {

      val tile = TreeLossTile(lossTile, tcd2010Tile)

      Raster(tile, window)
    }
  }
}

object TreeLossGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap.empty[String, TreeLossGridSources]

  def getCachedSources(grid: String): TreeLossGridSources = {

    cache.getOrElseUpdate(grid, TreeLossGridSources(grid))

  }

}