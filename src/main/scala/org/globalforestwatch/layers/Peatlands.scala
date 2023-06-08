package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class Peatlands(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {

//    val datasetName = "gfw_peatlands"
//
//    val uri: String =
//      uriForGrid(gridTile, kwargs)


  // For carbonflux package run only
  val datasetName = "Na"

  val uri: String =
    s"s3://gfw-files/flux_1_2_3/peatlands_flux_extent/standard/${gridTile.tileId}.tif"
}
