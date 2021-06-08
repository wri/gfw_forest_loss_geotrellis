package org.globalforestwatch.util

import com.vividsolutions.jts.geom.{Envelope, Polygon}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.spatialRDD.PolygonRDD
import org.globalforestwatch.grids.GridId.pointGridId
import org.globalforestwatch.util.GeoSparkGeometryConstructor.createPolygon1x1

object GridRDD {
  def apply(envelope: Envelope, spark: SparkSession): PolygonRDD = {

    val gridCells = {
      for (x <- envelope.getMinX.floor.toInt until envelope.getMaxX.ceil.toInt;
           y <- envelope.getMinY.floor.toInt until envelope.getMaxY.ceil.toInt)
        yield (x, y)
    }

    val gridRDD: RDD[Polygon] = {
      spark.sparkContext
        .parallelize(gridCells)
        .map { p =>
          val poly = createPolygon1x1(minX = p._1, minY = p._2)
          val gridId = pointGridId(p._1, p._2 + 1, 1)
          poly.setUserData(gridId)
          poly
        }

    }
    val spatialGridRDD = new PolygonRDD(gridRDD)
    spatialGridRDD.analyze()
    spatialGridRDD
  }


}
