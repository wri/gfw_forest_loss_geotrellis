package org.globalforestwatch.summarystats.carbonflux

import cats.implicits._
import geotrellis.contrib.polygonal._
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.vector._
import org.globalforestwatch.summarystats.SummaryRDD

object CarbonFluxRDD extends SummaryRDD {

  type SOURCES = CarbonFluxGridSources
  type SUMMARY = CarbonFluxSummary
  type TILE = CarbonFluxTile

  def getSources(window: Extent): Either[Throwable, SOURCES] = {
    Either.catchNonFatal {
      CarbonFluxGrid.getRasterSource(window)
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
      emptyResult = new CarbonFluxSummary(),
      options = options
    )
  }

}
