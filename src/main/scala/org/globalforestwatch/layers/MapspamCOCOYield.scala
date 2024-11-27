package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class MapspamCOCOYield(gridTile: GridTile, kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = "mapspam_coco_yield"

  val uri: String =
    uriForGrid(gridTile, kwargs)
}
