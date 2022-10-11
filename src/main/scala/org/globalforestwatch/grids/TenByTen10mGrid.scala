package org.globalforestwatch.grids

trait TenByTen10mGrid[T <: GridSources] extends Grid[T] {
  val pixelSize = 0.0001
  val gridSize = 10
  val rowCount = 100000
  val blockSize = 2000
}