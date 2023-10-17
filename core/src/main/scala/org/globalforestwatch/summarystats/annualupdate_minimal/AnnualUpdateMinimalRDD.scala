package org.globalforestwatch.summarystats.annualupdate_minimal

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.summary.polygonal._
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.vector._
import org.globalforestwatch.summarystats.SummaryRDD

object AnnualUpdateMinimalRDD extends SummaryRDD {

  type SOURCES = AnnualUpdateMinimalGridSources
  type SUMMARY = AnnualUpdateMinimalSummary
  type TILE = AnnualUpdateMinimalTile

  def getSources(windowKey: SpatialKey, windowLayout: LayoutDefinition, kwargs: Map[String, Any]): Either[Throwable, SOURCES] = {
    Either.catchNonFatal {
      AnnualUpdateMinimalGrid.getRasterSource(windowKey, windowLayout, kwargs)
    }
  }

  def readWindow(rs: SOURCES, windowKey: SpatialKey, windowLayout: LayoutDefinition): Either[Throwable, Raster[TILE]] =
    rs.readWindow(windowKey, windowLayout)

  def runPolygonalSummary(raster: Raster[TILE],
                          geometry: Geometry,
                          options: Rasterizer.Options,
                          kwargs: Map[String, Any]): PolygonalSummaryResult[SUMMARY] = {
    raster.polygonalSummary(
      geometry,
      AnnualUpdateMinimalSummary.getGridVisitor(kwargs),
      options = options
    )
  }

}
