package org.globalforestwatch.layers
import org.globalforestwatch.grids.GridTile

case class JplTropicsAbovegroundBiomassExtent2000(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {

  val datasetName = "Na"

  val uri: String =
    s"s3://gfw-files/2018_update/jpl_tropics_abovegroundbiomass_extent_2000/${gridTile.tileId}.tif"
}
