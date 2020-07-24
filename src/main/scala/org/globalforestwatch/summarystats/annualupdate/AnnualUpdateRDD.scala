package org.globalforestwatch.summarystats.annualupdate

import cats.implicits._
import geotrellis.raster.summary.polygonal._
import geotrellis.raster.summary.GridVisitor
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.vector._
import org.globalforestwatch.summarystats.SummaryRDD

object AnnualUpdateRDD extends SummaryRDD {

  type SOURCES = AnnualUpdateGridSources
  type SUMMARY = AnnualUpdateSummary
  type TILE = AnnualUpdateTile

  def getSources(window: Extent, kwargs:Map[String, Any]): Either[Throwable, SOURCES] = {
    Either.catchNonFatal {
      AnnualUpdateGrid.getRasterSource(window, kwargs)
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
      GridVisitor[Raster[AnnualUpdateTile], AnnualUpdateSummary],
      options = options
    )
  }

}
