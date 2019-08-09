package org.globalforestwatch.treecoverloss

import cats.implicits._
import geotrellis.contrib.polygonal._
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.vector._
import org.apache.spark.rdd.RDD
import org.globalforestwatch.features.SimpleFeatureId
import org.globalforestwatch.summarystats.SummaryRDD

object TreeLossRDD extends SummaryRDD {

  type SOURCES = TreeLossGridSources
  //  type FEATUREID = SimpleFeatureId
  type SUMMARY = TreeLossSummary
  type TILE = TreeLossTile

  def getSources(window: Extent): Either[Throwable, SOURCES] = {
    Either.catchNonFatal {
      TreeLossGrid.getRasterSource(window)
    }
  }

  def readWindow(rs: SOURCES,
                 window: Extent): Either[Throwable, Raster[TILE]] =
    rs.readWindow(window)

  def runPolygonalSummary(raster: Raster[TILE],
                          geometry: Geometry,
                          options: Rasterizer.Options,
                          tcdYear: Int): SUMMARY = {
    raster.polygonalSummary(
      geometry = geometry,
      emptyResult = new TreeLossSummary(tcdYear),
      options = options
    )
  }

  //  def reduceSummarybyKey(
  //                          featuresWithSummaries: RDD[(FEATUREID, SUMMARY)]
  //                        ): RDD[(FEATUREID, SUMMARY)] = {
  //    featuresWithSummaries.reduceByKey {
  //      case (summary1, summary2) =>
  //        summary1.merge(summary2)
  //    }
  //
  //  }

}
