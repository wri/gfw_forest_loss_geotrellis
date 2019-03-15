package org.globalforestwatch.treecoverloss

import org.scalatest.FunSuite
import Geodesy._

class GeodesySuits extends FunSuite {
  test("Geodesic Area Lat 0.0"){
    assert(pixelArea(0.0,0.00025, 0.00025) === 769.3170049535535)
  }

  test("Geodesic Area Lat 45.0"){
    assert(pixelArea(0.0,0.00025, 0.00025) === 547.6481292317709)
  }

  test("Geodesic Area Lat 90.0"){
    assert(pixelArea(0.0,0.00025, 0.00025) === 0.0017010416666666667)
  }

  test("Geodesic Area Lat -45.0"){
    assert(pixelArea(0.0,0.00025, 0.00025) === 547.65048671875)
  }

  test("Geodesic Area Lat -90.0"){
    assert(pixelArea(0.0,0.00025, 0.00025) === 0.0017010416666666667)
  }
}