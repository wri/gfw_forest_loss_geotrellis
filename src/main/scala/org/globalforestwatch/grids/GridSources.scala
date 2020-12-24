package org.globalforestwatch.grids

import com.typesafe.scalalogging.LazyLogging
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.{CellGrid, Raster}
import geotrellis.vector.Extent

trait GridSources extends LazyLogging {

  def readWindow(windowKey: SpatialKey, windowLayout: LayoutDefinition): Either[Throwable, Raster[CellGrid[Int]]]

}
