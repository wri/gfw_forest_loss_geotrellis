package org.globalforestwatch.layers

import cats.data.NonEmptyList
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.globalforestwatch.grids.GridTile
import org.globalforestwatch.summarystats.SummarySparkSession
import org.globalforestwatch.summarystats.carbonflux.CarbonFluxCommand.runAnalysis
import org.globalforestwatch.summarystats.gladalerts.GladAlertsAnalysis
import java.util.UUID.randomUUID

import java.time.LocalDate
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

  test("GLAD e2e test") {
    val resultsId = randomUUID()
    val outputUrl = s"file:///Users/Shared/dev/gfw_forest_loss_geotrellis/output/gladalerts_${resultsId}"

    val kwargs = Map(
      "outputUrl" -> outputUrl,
      "splitFeatures" -> false,
      "noOutputPathSuffix" -> true,
      "changeOnly" -> false,
      "iso" -> None,
      "isoFirst" -> None,
      "isoStart" -> None,
      "isoEnd" -> None,
      "admin1" -> None,
      "admin2" -> Some("IDN.24.9_1"),
      "idStart" -> None,
      "idEnd" -> None,
      "wdpaStatus" -> None,
      "iucnCat" -> None,
      "limit" -> None,
      "tcl" -> false,
      "glad" -> true
    )

    runAnalysis(
      "gladalerts",
      "gadm",
      NonEmptyList("file:///Users/Shared/dev/gfw_forest_loss_geotrellis/input/input.tsv", List()),
      kwargs
    )

    val spark: SparkSession = SummarySparkSession("Test")
    import spark.implicits._

    val df = spark.read
      .options(Map("header" -> "true", "delimiter" -> "\t", "escape" -> "\""))
      .csv(s"${outputUrl}/iso/weekly_alerts/*.csv")

    assert(df.agg(sum($"alert__count") as "alert__count").collect()(0)(0) == 1573079)
  }
}
