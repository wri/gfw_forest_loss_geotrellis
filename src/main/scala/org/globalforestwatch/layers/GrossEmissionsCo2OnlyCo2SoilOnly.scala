package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GrossEmissionsCo2OnlyCo2SoilOnly(gridTile: GridTile, kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = "Na"

  val uri: String =
      s"s3://gfw-files/flux_1_2_3/gross_emissions_co2_only_co2e/soil_only/${gridTile.tileId}.tif"

}
