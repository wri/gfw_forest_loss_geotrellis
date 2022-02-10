package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile


case class Agc2000(gridTile: GridTile, model: String = "standard", kwargs: Map[String, Any]) extends FloatLayer with OptionalFLayer {

    val datasetName = "Na"


    val uri: String =
    s"$basePath/gfw_aboveground_carbon_stock_2000/v20191106/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/Mg/gdal-geotiff/${gridTile.tileId}.tif"
        s"s3://gfw-files/flux_1_2_0/agc_2000/standard/${gridTile.tileId}.tif"
}
