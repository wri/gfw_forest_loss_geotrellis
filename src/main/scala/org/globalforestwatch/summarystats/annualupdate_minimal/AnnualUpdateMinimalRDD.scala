package org.globalforestwatch.summarystats.annualupdate_minimal

import cats.implicits._
import geotrellis.raster.summary.polygonal._
import geotrellis.raster.summary.GridVisitor
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.vector._
import org.globalforestwatch.summarystats.SummaryRDD

object AnnualUpdateMinimalRDD extends SummaryRDD {

  type SOURCES = AnnualUpdateMinimalGridSources
  type SUMMARY = AnnualUpdateMinimalSummary
  type TILE = AnnualUpdateMinimalTile

  def getSources(window: Extent, kwargs: Map[String, Any]): Either[Throwable, SOURCES] = {
    Either.catchNonFatal {
      AnnualUpdateMinimalGrid.getRasterSource(window, kwargs)
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
      GridVisitor[Raster[AnnualUpdateMinimalTile], AnnualUpdateMinimalSummary],
      options = options
    )
  }

}
