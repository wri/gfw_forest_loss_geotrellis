package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

case class GrossEmissionsCo2OnlyCo2e(gridTile: GridTile,
                                     model: String = "standard", kwargs: Map[String, Any])
  extends FloatLayer
    with OptionalFLayer {

  val datasetName = "gfw_full_extent_co2_gross_emissions"

  val uri: String =
    uriForGrid(gridTile, kwargs)
}
