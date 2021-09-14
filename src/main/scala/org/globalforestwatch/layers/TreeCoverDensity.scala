package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile
import org.globalforestwatch.config.GfwConfig

trait TreeCoverDensityThreshold extends IntegerLayer with RequiredILayer {

  override val externalNoDataValue: Integer = 0

  override def lookup(value: Int): Integer = {
    value match {
      case v if v <= 10 => 0
      case v if v <= 15 => 10
      case v if v <= 20 => 15
      case v if v <= 25 => 20
      case v if v <= 30 => 25
      case v if v <= 50 => 30
      case v if v <= 75 => 50
      case _ => 75
    }
  }
}

case class TreeCoverDensityThreshold2000(gridTile: GridTile)
  extends TreeCoverDensityThreshold {
  val uri: String = uriForGrid(GfwConfig.get.rasterLayers("TreeCoverDensity2000"), gridTile)
}

case class TreeCoverDensityThreshold2010(gridTile: GridTile)
  extends TreeCoverDensityThreshold {
  val uri: String = uriForGrid(GfwConfig.get.rasterLayers("TreeCoverDensity2010"), gridTile)
}

case class TreeCoverDensity2010_60(gridTile: GridTile)
  extends BooleanLayer
    with RequiredILayer {
  val uri: String = uriForGrid(GfwConfig.get.rasterLayers("TreeCoverDensity2010"), gridTile)

  override def lookup(value: Int): Boolean = value > 60

}

case class TreeCoverDensityPercent2000(gridTile: GridTile)
  extends IntegerLayer
    with RequiredILayer {
  override val externalNoDataValue: Integer = 0
  val uri: String = uriForGrid(GfwConfig.get.rasterLayers("TreeCoverDensity2000"), gridTile)
}

case class TreeCoverDensityPercent2010(gridTile: GridTile)
  extends IntegerLayer
    with RequiredILayer {
  override val externalNoDataValue: Integer = 0
  val uri: String = uriForGrid(GfwConfig.get.rasterLayers("TreeCoverDensity2010"), gridTile)
}
