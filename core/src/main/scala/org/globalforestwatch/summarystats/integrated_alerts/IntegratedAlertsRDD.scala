package org.globalforestwatch.summarystats.integrated_alerts

import cats.implicits._
import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.summary.polygonal._
import geotrellis.raster.summary.GridVisitor
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.vector._
import org.globalforestwatch.summarystats.SummaryRDD
import org.globalforestwatch.util.Util.getAnyMapValue
import org.globalforestwatch.util.{Geodesy, Mercantile}

import scala.annotation.tailrec

object IntegratedAlertsRDD extends SummaryRDD {

  type SOURCES = IntegratedAlertsGridSources
  type SUMMARY = IntegratedAlertsSummary
  type TILE = IntegratedAlertsTile

  def getSources(key: SpatialKey, windowLayout: LayoutDefinition, kwargs: Map[String, Any]): Either[Throwable, SOURCES] = {
    Either.catchNonFatal {
      IntegratedAlertsGrid.getRasterSource(key, windowLayout, kwargs)
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
      IntegratedAlertsSummary.getGridVisitor(kwargs),
      options = options
    )
  }
}
