package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class MexicoPaymentForEcosystemServices(gridTile: GridTile)
    extends BooleanLayer
    with OptionalILayer {
  val uri: String = s"$basePath/mex_payment_ecosystem_services/v20160613/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/is/gdal-geotiff/${gridTile.tileId}.tif"
}
