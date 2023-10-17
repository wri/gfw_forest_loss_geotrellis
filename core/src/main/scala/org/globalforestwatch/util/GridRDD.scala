package org.globalforestwatch.util

import org.locationtech.jts.geom.{Envelope, Geometry, GeometryFactory, MultiPolygon, Polygon}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.sedona.core.spatialRDD.PolygonRDD
import org.globalforestwatch.grids.GridId.pointGridId
import org.globalforestwatch.util.GeometryConstructor.createPolygon1x1

object GridRDD {
  def apply(envelope: Envelope, spark: SparkSession, clip: Boolean = false): PolygonRDD = {
    val gridCells = getGridCells(envelope)

    val tcl_geom: Geometry = TreeCoverLossExtent.geometry //new GeometryFactory().toGeometry(new Envelope(-180, 180, -90, 90))

    val gridRDD: RDD[Polygon] = {
      spark.sparkContext
        .parallelize(gridCells)
        .flatMap { p =>
          val poly = createPolygon1x1(minX = p._1, minY = p._2)
          val gridId = pointGridId(p._1, p._2 + 1, 1)
          poly.setUserData(gridId)
          if (clip)
            if (poly coveredBy tcl_geom) List(poly)
            else List()
          else List(poly)
        }

    }
    val spatialGridRDD = new PolygonRDD(gridRDD)
    spatialGridRDD.analyze()
    spatialGridRDD
  }

  private def getGridCells(envelope: Envelope): IndexedSeq[(Int, Int)] = {
    {
      for (x <- envelope.getMinX.floor.toInt until envelope.getMaxX.ceil.toInt;
           y <- envelope.getMinY.floor.toInt until envelope.getMaxY.ceil.toInt)
        yield (x, y)
    }
  }

}
