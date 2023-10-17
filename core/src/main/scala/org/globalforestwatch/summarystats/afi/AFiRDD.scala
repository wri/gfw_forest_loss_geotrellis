package org.globalforestwatch.summarystats.afi

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.summary.polygonal._
import geotrellis.vector._
import org.globalforestwatch.summarystats.ErrorSummaryRDD

object AFiRDD extends ErrorSummaryRDD {

  type SOURCES = AFiGridSources
  type SUMMARY = AFiSummary
  type TILE = AFiTile

  def getSources(windowKey: SpatialKey,
                 windowLayout: LayoutDefinition,
                 kwargs: Map[String, Any]): Either[Throwable, SOURCES] = {
    Either.catchNonFatal {
      AFiGrid.getRasterSource(
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
      AFiSummary.getGridVisitor(kwargs),
      options = options
    )
  }

}
