package org.globalforestwatch.layers

import org.globalforestwatch.grids.GridTile

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.scalatest.FunSuite

class GladAlertsSuits extends FunSuite {

  private val fullDate = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  val glad = new GladAlerts(GridTile(10, 40000, "10N_010E"))

  test("Unconfirmed date 1") {
    assert(
      glad.lookup(20001) == (LocalDate.parse("2015-01-01", fullDate), false)
    )
  }
  test("Unconfirmed date 2") {
    assert(
      glad.lookup(20366) == (LocalDate.parse("2016-01-01", fullDate), false)
    )
  }
  test("Unconfirmed date 3") {
    assert(
      glad.lookup(20732) == (LocalDate.parse("2017-01-01", fullDate), false)
    )
  }
  test("Confirmed date 1") {
    assert(
      glad.lookup(30001) == (LocalDate.parse("2015-01-01", fullDate), true)
    )
  }
  test("Confirmed date 2") {
    assert(
      glad.lookup(30366) == (LocalDate.parse("2016-01-01", fullDate), true)
    )
  }
  test("Confirmed date 3") {
    assert(
      glad.lookup(30732) == (LocalDate.parse("2017-01-01", fullDate), true)
    )
  }
}
