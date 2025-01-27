package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

/** Parameterized layer for all the various Mapspam commodity yield datasets.
  * 'commodity' should be one of the four-letter uppercase Mapspam commodity codes,
  * such as 'COCO' or 'OILP'.
  */
case class MapspamYield(commodity: String, gridTile: GridTile, kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = s"mapspam_yield_${commodity.toLowerCase()}"

  val uri: String =
    uriForGrid(gridTile, kwargs)
}
