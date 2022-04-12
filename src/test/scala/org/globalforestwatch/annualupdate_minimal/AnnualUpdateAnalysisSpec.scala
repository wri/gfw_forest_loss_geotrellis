package org.globalforestwatch.summarystats.annualupdate_minimal

import cats.data.NonEmptyList
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import geotrellis.vector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType}
import org.globalforestwatch.TestEnvironment
import org.globalforestwatch.features.{FeatureFilter, FeatureId, FeatureRDD, GfwProFeatureId, ValidatedFeatureRDD}
import org.globalforestwatch.summarystats.annualupdate_minimal.{AnnualUpdateMinimalAnalysis, AnnualUpdateMinimalDF}

class AnnualUpdateAnalysisSpec extends TestEnvironment with DataFrameComparer {
  def admin2Feature = getClass.getResource("/idn-24-9.tsv").toString()
  def admin2TclYear = getClass.getResource("/annualupdate-idn-24-9-output/tcl_by_year.csv").getPath

  def annualUpdate(features: RDD[Feature[Geometry, FeatureId]]) = {
    AnnualUpdateMinimalAnalysis(
      features,
      "gadm",
      spark,
      kwargs = Map.empty
    )
  }

  it("IDN/24/9 Annual Update") {
    val admin2RDD = FeatureRDD(
      NonEmptyList.one(admin2Feature),
      "gadm",
      FeatureFilter.empty,
      false,
      spark
    )

    import spark.implicits._

    val summaryDF = annualUpdate(admin2RDD)
    val unpackedDF =
      summaryDF.transform(
        AnnualUpdateMinimalDF.unpackValues(
          List($"id.iso" as "iso", $"id.adm1" as "adm1", $"id.adm2" as "adm2")
        )
      )

    val changeDF =
      unpackedDF.filter($"umd_tree_cover_loss__year".isNotNull &&
        ($"umd_tree_cover_loss__ha" > 0 || $"gfw_full_extent_gross_emissions__Mg_CO2e" > 0))
      .transform(AnnualUpdateMinimalDF.aggChange(List("iso", "adm1", "adm2")))

    changeDF.show()

    val expectedDF =
      spark.read
        .option("header", true)
        .csv(admin2TclYear)
        .withColumn("umd_tree_cover_loss__year", col("umd_tree_cover_loss__year").cast(LongType))
        .withColumn("umd_tree_cover_loss__ha", col("umd_tree_cover_loss__ha").cast(DoubleType))

    expectedDF.show()

    assertSmallDataFrameEquality(
      changeDF
        .select("umd_tree_cover_loss__year", "umd_tree_cover_loss__ha")
        .where(col("umd_tree_cover_density_2000__threshold") === 30)
        .where(col("umd_tree_cover_loss__year") < 2021)
        .groupBy("umd_tree_cover_loss__year")
        .sum(),
      expectedDF
    )
  }
}
