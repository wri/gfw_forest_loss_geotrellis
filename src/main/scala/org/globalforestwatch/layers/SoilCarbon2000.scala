package org.globalforestwatch.layers

case class SoilCarbon2000(grid: String, model: String="standard")
  extends FloatLayer
    with OptionalFLayer {
  val uri: String = s"$basePath/soil_carbon_2000/$model/$grid.tif"
}
