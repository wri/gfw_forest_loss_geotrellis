package org.globalforestwatch.layers
import org.globalforestwatch.grids.GridTile

case class ProdesLegalAmazonExtent2000(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {

  val datasetName = "Na"

  val uri: String =
    s"s3://gfw-files/2018_update/legal_Amazon_2000/${gridTile.tileId}.tif"

}
