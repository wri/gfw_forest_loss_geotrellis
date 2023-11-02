package org.globalforestwatch.layers
import org.globalforestwatch.grids.GridTile

case class TropicLatitudeExtent(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {

  val datasetName = "Na"

  val uri: String =
    s"s3://gfw-files/2018_update/tropic_latitude_extent/${gridTile.tileId}.tif"
}
