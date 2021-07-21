package org.globalforestwatch.layers
import org.globalforestwatch.grids.GridTile

case class JplTropicsAbovegroundBiomassDensity2000(gridTile: GridTile, kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = "Na"
  override lazy val version = "Na"

  val uri: String =
  //    s"$basePath/jpl_tropics_abovegroundbiomass_density_2000/v20190816/raster/epsg-4326/${gridTile.gridSize}/${gridTile.rowCount}/Mg_ha-1/geotiff/${gridTile.tileId}.tif"
    s"s3://gfw-files/2018_update/jpl_tropics_abovegroundbiomass_density_2000/Mg_ha-1/${gridTile.tileId}.tif"
}
