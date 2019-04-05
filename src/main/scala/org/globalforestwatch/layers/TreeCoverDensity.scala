package org.globalforestwatch.layers

trait TreeCoverDensity extends IntegerLayer with RequiredILayer {

  def lookup(density: Integer): Integer = {
    if (density <= 10) 0
    else if (density <= 15) 10
    else if (density <= 20) 15
    else if (density <= 25) 20
    else if (density <= 30) 25
    else if (density <= 50) 30
    else if (density <= 75) 50
    else 75
  }
}

class TreeCoverDensity2000(grid: String) extends TreeCoverDensity {
  val uri: String = s"$basePath/tcd_2000/$grid.tif"
}

class TreeCoverDensity2010(grid: String) extends TreeCoverDensity {
  val uri: String = s"$basePath/tcd_2010/$grid.tif"
}
