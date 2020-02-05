package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.grids.{Grid, GridSources}

trait ViirsGrid[T <: GridSources] extends Grid[T] {
  val pixelSize = (90.0 / 27008.0)
  val gridSize = 90
  val rowCount = 10
  val blockSize = 128
}
