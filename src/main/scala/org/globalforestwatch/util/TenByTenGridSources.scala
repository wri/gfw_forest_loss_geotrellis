package org.globalforestwatch.util

import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.{CellGrid, Raster}
import geotrellis.vector.Extent

trait TenByTenGridSources extends LazyLogging {

  def readWindow(window: Extent): Either[Throwable, Raster[CellGrid]]

}
