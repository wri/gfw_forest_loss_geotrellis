package org.globalforestwatch.summarystats.ghg

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.summary.polygonal._
import geotrellis.vector._
import org.globalforestwatch.summarystats.ErrorSummaryRDD

object GHGRDD extends ErrorSummaryRDD {

  type SOURCES = GHGGridSources
  type SUMMARY = GHGSummary
  type TILE = GHGTile

  def getSources(windowKey: SpatialKey,
                 windowLayout: LayoutDefinition,
                 kwargs: Map[String, Any]): Either[Throwable, SOURCES] = {
    Either.catchNonFatal {
      GHGGrid.getRasterSource(
        windowKey,
        windowLayout,
        kwargs
      )
    }.left.map { ex => logger.error("Error in GHGRDD.getSources", ex); ex}
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
      GHGSummary.getGridVisitor(kwargs),
      options = options
    )
  }

}
