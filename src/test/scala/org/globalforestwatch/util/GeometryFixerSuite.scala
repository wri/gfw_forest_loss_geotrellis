package org.globalforestwatch.util

import geotrellis.vector.{LineString, Polygon}
import org.globalforestwatch.util.GeotrellisGeometryValidator.makeValidGeom
import org.scalatest.funsuite.AnyFunSuite

class GeometryFixerSuite extends AnyFunSuite {

  test("A 'bowtie' polygon") {
    val poly: Polygon = Polygon((0.0, 0.0), (0.0, 10.0), (10.0, 0.0), (10.0, 10.0), (0.0, 0.0))
    val validGeom = makeValidGeom(poly)
    info(poly.toText())
    info(validGeom.toText())

    assert(!poly.isValid)
    assert(validGeom.isValid)
  }

  test("Inner ring with one edge sharing part of an edge of the outer ring") {
    val outerRing = LineString((0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0))
    val innerRing = LineString((5.0, 2.0), (5.0, 7.0), (10.0, 7.0), (10.0, 2.0), (5.0, 2.0))
    val poly: Polygon = Polygon(outerRing, innerRing)
    val validGeom = makeValidGeom(poly)

    assert(!poly.isValid)
    assert(validGeom.isValid)
    assert(validGeom.getGeometryType === poly.getGeometryType)
  }

  test("Dangling edge") {
    val poly: Polygon = Polygon((0.0, 0.0), (10.0, 0.0), (15.0, 5.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0))
    val validGeom = makeValidGeom(poly)

    assert(!poly.isValid)
    assert(validGeom.isValid)
    assert(validGeom.getGeometryType === poly.getGeometryType)
  }


  test("Two adjacent inner rings") {
    val outerRing = LineString((0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0))
    val innerRing1 = LineString((1.0, 1.0), (1.0, 8.0), (3.0, 8.0), (3.0, 1.0), (1.0, 1.0))
    val innerRing2 = LineString((3.0, 1.0), (3.0, 8.0), (5.0, 8.0), (5.0, 1.0), (3.0, 1.0))

    val poly: Polygon = Polygon(outerRing, innerRing1, innerRing2)
    val validGeom = makeValidGeom(poly)

    assert(!poly.isValid)
    assert(validGeom.isValid)
    assert(validGeom.getGeometryType === poly.getGeometryType)
  }

  test("Non overlapping Holes") {
    val outerRing = LineString((10.0, 90.0), (90.0, 90.0), (90.0, 10.0), (10.0, 10.0), (10.0, 90.0))
    val innerRing1 = LineString((80.0, 70.0), (30.0, 70.0), (30.0, 20.0), (30.0, 70.0), (80.0, 70.0))
    val innerRing2 = LineString((70.0, 80.0), (70.0, 30.0), (20.0, 30.0), (70.0, 30.0), (70.0, 80.0))

    val poly: Polygon = Polygon(outerRing, innerRing1, innerRing2)
    val validGeom = makeValidGeom(poly)

    assert(!poly.isValid)
    assert(validGeom.isValid)
    assert(validGeom.getGeometryType === poly.getGeometryType)
  }

  test("Polygon with an inner ring inside another inner ring") {
    val outerRing = LineString((0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0))
    val innerRing1 = LineString((2.0, 8.0), (5.0, 8.0), (5.0, 2.0), (2.0, 2.0), (2.0, 8.0))
    val innerRing2 = LineString((3.0, 3.0), (4.0, 3.0), (3.0, 4.0), (3.0, 3.0))

    val poly: Polygon = Polygon(outerRing, innerRing1, innerRing2)
    val validGeom = makeValidGeom(poly)

    assert(!poly.isValid)
    assert(validGeom.isValid)
    assert(validGeom.getGeometryType === poly.getGeometryType)
  }

  test("Positive and negative overlap") {
    val poly: Polygon = Polygon((10.0, 90.0), (50.0, 90.0), (50.0, 30.0), (70.0, 30.0), (70.0, 50.0), (30.0, 50.0), (30.0, 70.0),
      (90.0, 70.0), (90.0, 10.0), (10.0, 10.0), (10.0, 90.0))
    val validGeom = makeValidGeom(poly)

    assert(!poly.isValid)
    assert(validGeom.isValid)
    assert(validGeom.getGeometryType === poly.getGeometryType)
  }

}
