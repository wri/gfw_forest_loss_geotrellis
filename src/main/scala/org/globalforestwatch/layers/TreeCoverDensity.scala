package org.globalforestwatch.layers

trait TreeCoverDensity extends IntegerLayer with RequiredILayer {

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

case class TreeCoverDensityThresholds2000(grid: String)
  extends TreeCoverDensity {
  val uri: String = s"$basePath/tcd_2000/$grid.tif"
}

case class TreeCoverDensityThresholds2010(grid: String)
  extends TreeCoverDensity {
  val uri: String = s"$basePath/tcd_2010/$grid.tif"
}

case class TreeCoverDensity2010_60(grid: String)
  extends BooleanLayer
    with RequiredILayer {
  val uri: String = s"$basePath/tcd_2010/$grid.tif"

  override def lookup(value: Int): Boolean = {
    if (value <= 60) false
    else true
  }
}

case class TreeCoverDensity2000(grid: String)
  extends IntegerLayer
    with RequiredILayer {
  override val externalNoDataValue: Integer = 0
  val uri: String = s"$basePath/tcd_2000/$grid.tif"
}

case class TreeCoverDensity2010(grid: String)
  extends IntegerLayer
    with RequiredILayer {
  override val externalNoDataValue: Integer = 0
  val uri: String = s"$basePath/tcd_2010/$grid.tif"
}
