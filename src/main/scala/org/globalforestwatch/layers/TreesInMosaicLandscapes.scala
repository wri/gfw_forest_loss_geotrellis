package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class TreesInMosaicLandscapes(gridTile: GridTile, kwargs: Map[String, Any])
  extends IntegerLayer
    with OptionalILayer {
  val datasetName = "wri_trees_in_mosaic_landscapes"
  val uri: String =
    uriForGrid(gridTile)
}
