package org.globalforestwatch.util

import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory, Polygon, PrecisionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.spatialRDD.PolygonRDD
import org.globalforestwatch.grids.GridId.pointGridId

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
        .map(p => createPolygon(minX = p._1, minY = p._2))

    }
    val spatialGridRDD = new PolygonRDD(gridRDD)
    spatialGridRDD.analyze()
    spatialGridRDD
  }

  private def createPolygon(minX: Double, minY: Double): Polygon = {

    val geomFactory = new GeometryFactory(new PrecisionModel(), 4326)

    val coordSeqFactory = geomFactory.getCoordinateSequenceFactory

    val polygon = geomFactory.createPolygon(
      coordSeqFactory.create(
        Array(
          new Coordinate(minX, minY),
          new Coordinate(minX + 1, minY),
          new Coordinate(minX + 1, minY + 1),
          new Coordinate(minX, minY + 1),
          new Coordinate(minX, minY)
        )
      )
    )

    // Somehow this doesn't work
    polygon.setSRID(geomFactory.getSRID)
    // polygon.setUserData(pointGridId(minX, minY+1, 1))
    polygon
  }

}
