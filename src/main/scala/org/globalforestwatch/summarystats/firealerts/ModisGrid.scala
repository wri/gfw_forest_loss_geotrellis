package org.globalforestwatch.summarystats.firealerts

import org.globalforestwatch.grids.{Grid, GridSources}

trait ModisGrid[T <: GridSources] extends Grid[T] {
  val pixelSize = (90.0 / 9984.0)
  val gridSize = 90
  val rowCount = 10
  val blockSize = 416
}
