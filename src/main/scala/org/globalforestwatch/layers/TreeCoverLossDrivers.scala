package org.globalforestwatch.layers

case class TreeCoverLossDrivers(grid: String)
    extends StringLayer
    with OptionalILayer {
  val uri: String = s"$basePath/drivers/v2019/$grid.tif"

  override val internalNoDataValue = 0
  override val externalNoDataValue = "Unknown"

  def lookup(value: Int): String = value match {
    case 2 => "Commodity driven deforestation"
    case 3 => "Shifting agriculture"
    case 4 => "Forestry"
    case 5 => "Wildfire"
    case 6 => "Urbanization"
    case _ => "Unknown"
  }
}
