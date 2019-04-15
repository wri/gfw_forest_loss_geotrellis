package org.globalforestwatch.layers

trait TreeCoverDensity extends IntegerLayer with RequiredILayer {

  override val externalNoDataValue: Integer = 0

  override def lookup(value: Int): Integer = {
    if (value <= 12) 0
    else if (value <= 17) 10
    else if (value <= 22) 15
    else if (value <= 27) 20
    else if (value <= 32) 25
    else if (value <= 52) 30
    else if (value <= 77) 50
    else 75
  }
}

class TreeCoverDensity2000(grid: String) extends TreeCoverDensity {
  val uri: String = s"$basePath/tcd_2000/$grid.tif"
}

class TreeCoverDensity2010(grid: String) extends TreeCoverDensity {
  val uri: String = s"$basePath/tcd_2010/$grid.tif"
}
