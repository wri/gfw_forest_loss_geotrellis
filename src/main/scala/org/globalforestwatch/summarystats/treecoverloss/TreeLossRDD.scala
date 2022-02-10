package org.globalforestwatch.summarystats.treecoverloss

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.summary.polygonal._
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.vector._
import org.globalforestwatch.summarystats.SummaryRDD

object TreeLossRDD extends SummaryRDD {

  type SOURCES = TreeLossGridSources
  type SUMMARY = TreeLossSummary
  type TILE = TreeLossTile

  def getSources(windowKey: SpatialKey, windowLayout: LayoutDefinition, kwargs: Map[String, Any]): Either[Throwable, SOURCES] = {
    Either.catchNonFatal {
      TreeLossGrid.getRasterSource(windowKey, windowLayout, kwargs)
    }
  }

  def readWindow(rs: SOURCES,
                 windowKey: SpatialKey, windowLayout: LayoutDefinition): Either[Throwable, Raster[TILE]] =
    rs.readWindow(windowKey, windowLayout)

  def runPolygonalSummary(raster: Raster[TILE],
                          geometry: Geometry,
                          options: Rasterizer.Options,
                          kwargs: Map[String, Any]): PolygonalSummaryResult[SUMMARY] = {
    raster.polygonalSummary(
      geometry,
      TreeLossSummary.getGridVisitor(kwargs),
      options = options
    )
  }

}
