package org.globalforestwatch.grids

import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.{CellGrid, Raster}
import geotrellis.vector.Extent

trait GridSources extends LazyLogging {

  def readWindow(window: Extent): Either[Throwable, Raster[CellGrid]]

}
