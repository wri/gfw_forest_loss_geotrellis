package org.globalforestwatch.util

import com.vividsolutions.jts.geom.Envelope
import org.apache.spark.sql.SparkSession
import org.globalforestwatch.summarystats.SummarySparkSession
import org.scalatest.funsuite.AnyFunSuite

class GridRDDSuite extends AnyFunSuite {

  val spark: SparkSession = SummarySparkSession("Test")

  test("Create single grid cell") {
    val envelope = new Envelope(1.0, 2.0, 0.0, 1.0)
    val gridRDD = GridRDD(envelope, spark)
    assert(gridRDD.countWithoutDuplicates() === 1)
    val geom = gridRDD.rawSpatialRDD.take(1).get(0)
    assert(geom.getArea === 1.0)
    assert(geom.getGeometryType === "Polygon")
    //    assert(geom.getSRID === 4326)
    assert(geom.getCentroid.getX === 1.5)
    assert(geom.getCentroid.getY === 0.5)
  }

  test("Create multiple grid cells") {
    val envelope = new Envelope(0.0, 2.0, 0.0, 2.0)
    val gridRDD = GridRDD(envelope, spark)
    assert(gridRDD.countWithoutDuplicates() === 4)

  }
}
