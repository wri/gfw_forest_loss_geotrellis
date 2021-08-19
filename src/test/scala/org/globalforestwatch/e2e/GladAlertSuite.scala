package org.globalforestwatch.e2e

import cats.data.NonEmptyList
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.globalforestwatch.summarystats.SummarySparkSession
import org.globalforestwatch.summarystats.carbonflux.CarbonFluxCommand.runAnalysis

import java.util.UUID.randomUUID
import org.scalatest.funsuite.AnyFunSuite

class GladAlertSuite extends AnyFunSuite {
  test("GADM") {
    // Create a random ID to store the results in a unique folder
    val resultsId = randomUUID()
    val outputUrl = s"src/test/scala/org/globalforestwatch/output/gladalerts_${resultsId}"

    // define the keyword args sent via the command manually
    val kwargs = Map(
      "outputUrl" -> outputUrl,
      "splitFeatures" -> false,
      "noOutputPathSuffix" -> true, // disable the auto-suffix using time
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

    // Directly call the analysis function to run the analysis and export the results
    runAnalysis(
      "gladalerts",
      "gadm",
      NonEmptyList("input/input.tsv", List()),
      kwargs
    )

    // Create a new spark session to analyze the results
    val spark: SparkSession = SummarySparkSession("Test")

    // This adds some special functions at runtime we use below
    import spark.implicits._

    // Read a directory of CSV files into a Spark DataFrame
    val df = spark.read
      .options(Map("header" -> "true", "delimiter" -> "\t", "escape" -> "\""))
      .csv(s"${outputUrl}/adm2/daily_alerts/*.csv")

    // There's two ways to query Spark DataFrames. Feel free to choose which you prefer.

    /*
      Option 1: Use DataFrame API, which is similar to pandas but slightly different.
      The '$' in front of the column names turns them into Column objects, which is the API
      works on. Note for equality checks in the where function, you need to use triple equals.
     */
    val resultDf1 = df
      .where($"is__umd_regional_primary_forest_2001" === true)
      .where($"is__confirmed_alert" === true)
      .where($"alert__date" < "2021-01-01")
      .agg(sum($"alert__count") as "alert__count")

    // This will print out a preview of the results
    resultDf1.show()

    /*
      Option 2: Use direct SQL, which should look the same as API queries we do.
      To do this, you have to the createOrReplaceTempView method on the DF first.
      This create the name you can use in the FROM statement. To be consistent, let's
      use the names we currently use for the tables in the API.
     */
    df.createOrReplaceTempView("gadm__glad__iso_weekly_alerts")
    val resultDf2 = spark.sql(
      s"""
         |SELECT sum(alert__count) as alert__count
         |FROM gadm__glad__iso_weekly_alerts
         |WHERE is__umd_regional_primary_forest_2001 = 'true'
         |      AND is__confirmed_alert = 'true'
         |      AND alert__date < '2021-01-01'""".stripMargin)

    resultDf2.show()

    /*
      Spark runs lazily, which means it won't actually run any of the aggregation
      above until you tell it you need the result. The collect() function will make
      it actually run the SQL, and return the results of an Array of Rows. In this case,
      we aggregated to just one total number, so we can access the first element of the first
      row with (0)(0) to get the result.
    */

    val actualResult = resultDf2.collect()(0)(0)
    val expectedResult = 546317

    assert(actualResult == expectedResult)
  }
}
