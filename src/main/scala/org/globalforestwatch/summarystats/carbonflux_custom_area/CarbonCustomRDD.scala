package org.globalforestwatch.summarystats.carbonflux_custom_area

import cats.implicits._
import geotrellis.contrib.polygonal._
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.vector._
import org.globalforestwatch.summarystats.SummaryRDD

object CarbonCustomRDD extends SummaryRDD {

  type SOURCES = CarbonCustomGridSources
  type SUMMARY = CarbonCustomSummary
  type TILE = CarbonCustomTile

  def getSources(window: Extent, kwargs: Map[String, Any]): Either[Throwable, SOURCES] = {
    Either.catchNonFatal {
      CarbonCustomGrid.getRasterSource(window, kwargs)
    }
  }

  def readWindow(rs: SOURCES, window: Extent): Either[Throwable, Raster[TILE]] =
    rs.readWindow(window)

  def runPolygonalSummary(raster: Raster[TILE],
                          geometry: Geometry,
                          options: Rasterizer.Options,
                          kwargs: Map[String, Any]): SUMMARY = {
    raster.polygonalSummary(
      geometry = geometry,
      emptyResult = new CarbonCustomSummary(),
      options = options
    )
  }

}
