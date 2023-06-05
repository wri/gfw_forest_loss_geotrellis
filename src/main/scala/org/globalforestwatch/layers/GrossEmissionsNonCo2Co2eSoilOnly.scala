package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GrossEmissionsNonCo2Co2eSoilOnly(gridTile: GridTile, kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = "Na"


  val uri: String =
      s"s3://gfw-files/flux_1_2_2/gross_emissions_non_co2_co2e/soil_only/${gridTile.tileId}.tif"
}