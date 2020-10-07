package org.globalforestwatch.summarystats.carbonflux

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.summary.GridVisitor
import geotrellis.raster.summary.polygonal._
import geotrellis.vector._
import org.globalforestwatch.summarystats.SummaryRDD

object CarbonFluxRDD extends SummaryRDD {

  type SOURCES = CarbonFluxGridSources
  type SUMMARY = CarbonFluxSummary
  type TILE = CarbonFluxTile

  def getSources(windowKey: SpatialKey, windowLayout: LayoutDefinition, kwargs: Map[String, Any]): Either[Throwable, SOURCES] = {
    Either.catchNonFatal {
      CarbonFluxGrid.getRasterSource(windowKey, windowLayout, kwargs)
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
      GridVisitor[Raster[CarbonFluxTile], CarbonFluxSummary],
      options = options
    )
  }

}
