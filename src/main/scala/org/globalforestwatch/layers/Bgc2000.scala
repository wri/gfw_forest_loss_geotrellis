package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class Bgc2000(gridTile: GridTile, model: String = "standard") extends FloatLayer with OptionalFLayer {
 val model_suffix: String = if (model == "standard") "standard" else s"$model"
    val uri: String =
  //  s"$basePath/gfw_belowground_carbon_stock_2000${model_suffix}/v20191106/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/Mg/gdal-geotiff/${gridTile.tileId}.tif"
      s"s3://gfw-files/flux_1_2_0/bgc_2000/standard/${gridTile.tileId}.tif"
}
