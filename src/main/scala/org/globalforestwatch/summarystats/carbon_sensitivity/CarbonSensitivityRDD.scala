package org.globalforestwatch.summarystats.carbon_sensitivity

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.summary.polygonal._
import geotrellis.raster.summary.GridVisitor
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.vector._
import org.globalforestwatch.summarystats.SummaryRDD

object CarbonSensitivityRDD extends SummaryRDD {

  type SOURCES = CarbonSensitivityGridSources
  type SUMMARY = CarbonSensitivitySummary
  type TILE = CarbonSensitivityTile

  def getSources(windowKey: SpatialKey, windowLayout: LayoutDefinition, kwargs: Map[String, Any]): Either[Throwable, SOURCES] = {
    Either.catchNonFatal {
      CarbonSensitivityGrid.getRasterSource(windowKey, windowLayout, kwargs)
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
      GridVisitor[Raster[CarbonSensitivityTile], CarbonSensitivitySummary],
      options = options
    )
  }

}
