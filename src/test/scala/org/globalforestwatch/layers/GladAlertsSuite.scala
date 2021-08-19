package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile
import java.time.format.DateTimeFormatter
import org.scalatest.funsuite.AnyFunSuite

class GladAlertsSuits extends AnyFunSuite {

  private val fullDate = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  val glad = new GladAlerts(GridTile(10, 40000, 400, "10N_010E"))

  test("Unconfirmed date 1") {
    assert(
      glad.lookup(20001) == Some(("2015-01-01", false))
    )
  }
  test("Unconfirmed date 2") {
    assert(
      glad.lookup(20366) === Some(("2016-01-01", false))
    )
  }
  test("Unconfirmed date 3") {
    assert(
      glad.lookup(20732) === Some(("2017-01-01", false))
    )
  }
  test("Confirmed date 1") {
    assert(
      glad.lookup(30001) === Some(("2015-01-01", true))
    )
  }
  test("Confirmed date 2") {
    assert(
      glad.lookup(30366) === Some(("2016-01-01", true))
    )
  }
  test("Confirmed date 3") {
    assert(
      glad.lookup(30732) === Some(("2017-01-01", true))
    )
  }
}
