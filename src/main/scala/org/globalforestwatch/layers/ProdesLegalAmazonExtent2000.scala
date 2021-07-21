package org.globalforestwatch.layers
import org.globalforestwatch.grids.GridTile

case class ProdesLegalAmazonExtent2000(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {

  val datasetName = "Na"
  override lazy val version = "Na"

  val uri: String =
  //    s"$basePath/prodes_legal_Amazon_2000/v20190816/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/geotiff/${gridTile.tileId}.tif"
    s"s3://gfw-files/2018_update/legal_Amazon_2000/${gridTile.tileId}.tif"

}
