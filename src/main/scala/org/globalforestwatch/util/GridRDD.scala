package org.globalforestwatch.util

import com.vividsolutions.jts.geom.{Envelope, MultiPolygon, Polygon}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.spatialRDD.PolygonRDD
import org.globalforestwatch.grids.GridId.pointGridId
import org.globalforestwatch.util.GeoSparkGeometryConstructor.createPolygon1x1
import org.globalforestwatch.util.ImplicitGeometryConverter._

object GridRDD {
  def apply(envelope: Envelope, spark: SparkSession, clip: Boolean = false): PolygonRDD = {

    val gridCells = {
      for (x <- envelope.getMinX.floor.toInt until envelope.getMaxX.ceil.toInt;
           y <- envelope.getMinY.floor.toInt until envelope.getMaxY.ceil.toInt)
        yield (x, y)
    }

    val tcl_geom: MultiPolygon = TreeCoverLossExtent.geometry

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

}