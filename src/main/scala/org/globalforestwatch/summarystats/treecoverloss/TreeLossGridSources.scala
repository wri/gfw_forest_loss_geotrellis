package org.globalforestwatch.summarystats.treecoverloss

import geotrellis.raster.Raster
import geotrellis.vector.Extent
import cats.implicits._
import org.globalforestwatch.grids.{GridSources, GridTile}
import org.globalforestwatch.layers._

/**
  * @param gridTile top left corner, padded from east ex: "10N_010E"
  */
case class TreeLossGridSources(gridTile: GridTile) extends GridSources {

  val treeCoverLoss = TreeCoverLoss(gridTile)
  val treeCoverGain = TreeCoverGain(gridTile)
  val treeCoverDensity2000 = TreeCoverDensity2000(gridTile)
  val treeCoverDensity2010 = TreeCoverDensity2010(gridTile)
  val biomassPerHectar = BiomassPerHectar(gridTile)
  val primaryForest = PrimaryForest(gridTile)


  def readWindow(window: Extent): Either[Throwable, Raster[TreeLossTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      lossTile <- Either.catchNonFatal(treeCoverLoss.fetchWindow(window)).right
      gainTile <- Either.catchNonFatal(treeCoverGain.fetchWindow(window)).right
      tcd2000Tile <- Either
        .catchNonFatal(treeCoverDensity2000.fetchWindow(window))
        .right
      tcd2010Tile <- Either
        .catchNonFatal(treeCoverDensity2010.fetchWindow(window))
        .right

    } yield {
      // Failure for these will be converted to optional result and propagated with TreeLossTile
      val biomassTile = biomassPerHectar.fetchWindow(window)
      val primaryForestTile = primaryForest.fetchWindow(window)


      val tile = TreeLossTile(
        lossTile,
        gainTile,
        tcd2000Tile,
        tcd2010Tile,
        biomassTile,
        primaryForestTile
      )

      Raster(tile, window)
    }
  }
}

object TreeLossGridSources {

  @transient
  private lazy val cache =
    scala.collection.concurrent.TrieMap.empty[String, TreeLossGridSources]

  def getCachedSources(gridTile: GridTile): TreeLossGridSources = {

    cache.getOrElseUpdate(gridTile.tileId, TreeLossGridSources(gridTile))

  }

}
