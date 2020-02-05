package org.globalforestwatch.summarystats.firealerts

import cats.implicits._
import geotrellis.contrib.polygonal._
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector._
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.features.{FireAlertFeatureId, FeatureId}
import org.globalforestwatch.summarystats.SummaryRDD

import scala.reflect.ClassTag

object FireAlertsRDD extends SummaryRDD {

  type SOURCES = FireAlertsGridSources
  type SUMMARY = FireAlertsSummary
  type TILE = FireAlertsTile

  def getSources(window: Extent, kwargs: Map[String, Any]): Either[Throwable, SOURCES] = {
    Either.catchNonFatal {
      FireAlertsGrid.getRasterSource(window, kwargs)
    }
  }

  def readWindow(rs: SOURCES, window: Extent): Either[Throwable, Raster[TILE]] =
    rs.readWindow(window)

  def runPolygonalSummary(raster: Raster[TILE],
                          geometry: Geometry,
                          options: Rasterizer.Options,
                          kwargs: Map[String, Any]): SUMMARY = {
    raster.polygonalSummary(
      geometry = geometry,
      emptyResult = new FireAlertsSummary(kwargs = kwargs),
      options = options
    )
  }

}
