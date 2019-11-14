package org.globalforestwatch.layers

case class SoilCarbon2000(grid: String, model: String="standard")
    extends DoubleLayer
      with OptionalDLayer {
  val uri: String = s"$basePath/soil_carbon_2000/$model/$grid.tif"
}
