package org.globalforestwatch.treecoverloss


import org.scalatest.FunSuite
import geotrellis.vector.Point
import TreeLossGrid._


class TreeLossGridSuits extends FunSuite {

  test("pointGridId")  {

    assert(pointGridId(Point(14,1))==="10N_010E")

    assert(pointGridId(Point(14,11))==="20N_010E")

    assert(pointGridId(Point(4,1))==="10N_000E")

    assert(pointGridId(Point(24,1))==="10N_020E")

    assert(pointGridId(Point(-14,-1))==="00N_020W")

    assert(pointGridId(Point(-14,-11))==="10S_020W")

    assert(pointGridId(Point(-4,-1))==="00N_010W")

    assert(pointGridId(Point(-24,-1))==="00N_030W")

  }

}
