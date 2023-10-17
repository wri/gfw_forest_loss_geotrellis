package org.globalforestwatch.layers
import org.globalforestwatch.grids.GridTile

case class MangroveBiomassExtent(gridTile: GridTile, kwargs: Map[String, Any])
  extends BooleanLayer
    with OptionalILayer {

  val datasetName = "jpl_mangrove_aboveground_biomass_stock_2000"

  val uri: String =
    uriForGrid(gridTile, kwargs)

}
