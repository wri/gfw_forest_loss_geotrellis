package org.globalforestwatch.layers
import org.globalforestwatch.grids.GridTile

case class JplTropicsAbovegroundBiomassDensity2000(gridTile: GridTile, kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = "Na"

  val uri: String =
    s"s3://gfw-files/2018_update/jpl_tropics_abovegroundbiomass_density_2000/Mg_ha-1/${gridTile.tileId}.tif"
}
