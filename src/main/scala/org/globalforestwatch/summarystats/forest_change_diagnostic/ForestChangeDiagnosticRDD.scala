package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.summary.polygonal._
import geotrellis.vector._
import org.globalforestwatch.summarystats.SummaryRDD

object ForestChangeDiagnosticRDD extends SummaryRDD {

  type SOURCES = ForestChangeDiagnosticGridSources
  type SUMMARY = ForestChangeDiagnosticSummary
  type TILE = ForestChangeDiagnosticTile

  def getSources(windowKey: SpatialKey,
                 windowLayout: LayoutDefinition,
                 kwargs: Map[String, Any]): Either[Throwable, SOURCES] = {
    Either.catchNonFatal {
      ForestChangeDiagnosticGrid.getRasterSource(
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
      ForestChangeDiagnosticSummary.getGridVisitor(kwargs),
      options = options
    )
  }

}
