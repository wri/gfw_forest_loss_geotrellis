package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.summary.polygonal._
import geotrellis.vector._
import org.globalforestwatch.summarystats.ErrorSummaryRDD

object GfwProDashboardRDD extends ErrorSummaryRDD {

  type SOURCES = GfwProDashboardGridSources
  type SUMMARY = GfwProDashboardSummary
  type TILE = GfwProDashboardTile

  def getSources(windowKey: SpatialKey,
                 windowLayout: LayoutDefinition,
                 kwargs: Map[String, Any]): Either[Throwable, SOURCES] = {
    Either.catchNonFatal {
      GfwProDashboardGrid.getRasterSource(
        windowKey,
        windowLayout,
        kwargs
      )
    }
  }

  def readWindow(
                  rs: SOURCES,
                  windowKey: SpatialKey,
                  windowLayout: LayoutDefinition
                ): Either[Throwable, Raster[TILE]] =
    rs.readWindow(windowKey, windowLayout)

  def runPolygonalSummary(
                           raster: Raster[TILE],
                           geometry: Geometry,
                           options: Rasterizer.Options,
                           kwargs: Map[String, Any]
                         ): PolygonalSummaryResult[SUMMARY] = {
    raster.polygonalSummary(
      geometry,
      GfwProDashboardSummary.getGridVisitor(kwargs),
      options = options
    )
  }

}
