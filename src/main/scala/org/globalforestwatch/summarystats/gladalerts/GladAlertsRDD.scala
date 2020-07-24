package org.globalforestwatch.summarystats.gladalerts

import cats.implicits._
import geotrellis.raster.summary.polygonal._
import geotrellis.raster.summary.GridVisitor
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.vector._
import org.globalforestwatch.summarystats.SummaryRDD

object GladAlertsRDD extends SummaryRDD {

  type SOURCES = GladAlertsGridSources
  type SUMMARY = GladAlertsSummary
  type TILE = GladAlertsTile

  def getSources(window: Extent, kwargs: Map[String, Any]): Either[Throwable, SOURCES] = {
    Either.catchNonFatal {
      GladAlertsGrid.getRasterSource(window, kwargs)
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
      GridVisitor[Raster[GladAlertsTile], GladAlertsSummary],
      options = options
    )
  }
}
