package org.globalforestwatch.layers

import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.grids.GridTile

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.scalatest.funsuite.AnyFunSuite

class GladAlertsSuits extends AnyFunSuite {

  private val fullDate = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  val glad = new GladAlerts(GridTile(10, 40000, 400, "10N_010E"), Map("config" -> GfwConfig.get))

  test("Unconfirmed date 1") {
    assert(
      glad.lookup(20001) == Some((LocalDate.of(2015, 1, 1), false))
    )
  }
  test("Unconfirmed date 2") {
    assert(
      glad.lookup(20366) === Some((LocalDate.of(2016,1,1), false))
    )
  }
  test("Unconfirmed date 3") {
    assert(
      glad.lookup(20732) === Some((LocalDate.of(2017,1,1), false))
    )
  }
  test("Confirmed date 1") {
    assert(
      glad.lookup(30001) === Some((LocalDate.of(2015,1,1), true))
    )
  }
  test("Confirmed date 2") {
    assert(
      glad.lookup(30366) === Some((LocalDate.of(2016,1,1), true))
    )
  }
  test("Confirmed date 3") {
    assert(
      glad.lookup(30732) === Some((LocalDate.of(2017,1,1), true))
    )
  }
}
