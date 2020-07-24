package org.globalforestwatch.summarystats.carbonflux

import cats.implicits._
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.summary.GridVisitor
import geotrellis.raster.summary.polygonal._
import geotrellis.raster.summary.polygonal.PolygonalSummaryResult
import geotrellis.vector._
import org.globalforestwatch.summarystats.SummaryRDD

object CarbonFluxRDD extends SummaryRDD {

  type SOURCES = CarbonFluxGridSources
  type SUMMARY = CarbonFluxSummary
  type TILE = CarbonFluxTile

  def getSources(window: Extent, kwargs: Map[String, Any]): Either[Throwable, SOURCES] = {
    Either.catchNonFatal {
      CarbonFluxGrid.getRasterSource(window, kwargs)
    }
  }

  def readWindow(rs: SOURCES, window: Extent): Either[Throwable, Raster[TILE]] =
    rs.readWindow(window)

  def runPolygonalSummary(raster: Raster[TILE],
                          geometry: Geometry,
                          options: Rasterizer.Options,
                          kwargs: Map[String, Any]): PolygonalSummaryResult[SUMMARY] = {
    raster.polygonalSummary(
      geometry,
      GridVisitor[Raster[CarbonFluxTile], CarbonFluxSummary],
      options = options
    )
  }

}
