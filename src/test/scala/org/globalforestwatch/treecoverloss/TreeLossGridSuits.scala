package org.globalforestwatch.treecoverloss


import org.scalatest.FunSuite
import geotrellis.vector.Point
import org.globalforestwatch.grids.GridId._


class TreeLossGridSuits extends FunSuite {

  test("pointGridId")  {

    val gridSize = TreeLossGrid.gridSize

    assert(pointGridId(Point(14, 1), gridSize) === "10N_010E")

    assert(pointGridId(Point(14, 11), gridSize) === "20N_010E")

    assert(pointGridId(Point(4, 1), gridSize) === "10N_000E")

    assert(pointGridId(Point(24, 1), gridSize) === "10N_020E")

    assert(pointGridId(Point(-14, -1), gridSize) === "00N_020W")

    assert(pointGridId(Point(-14, -11), gridSize) === "10S_020W")

    assert(pointGridId(Point(-4, -1), gridSize) === "00N_010W")

    assert(pointGridId(Point(-24, -1), gridSize) === "00N_030W")

  }

}
