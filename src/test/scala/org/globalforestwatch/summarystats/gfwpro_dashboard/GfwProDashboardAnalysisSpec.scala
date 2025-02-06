package org.globalforestwatch.summarystats.gfwpro_dashboard

import cats.data.NonEmptyList
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import geotrellis.vector._
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType,BooleanType}
import org.globalforestwatch.features.{FeatureFilter, ValidatedFeatureRDD}
import org.globalforestwatch.summarystats.ValidatedLocation
import org.globalforestwatch.{TestEnvironment, ProTag}
import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.util.Config

class GfwProDashboardAnalysisSpec extends TestEnvironment with DataFrameComparer {
  def dashInputTsvPath = getClass.getResource("/dash.tsv").toString()
  def dashExpectedOutputPath = getClass.getResource("/dash-output").toString()
  // This is a partial version of gadm36_adm2_1_1.tsv that only includes IDN.1.5
  def idn1_5GadmTsvPath = getClass.getResource("/idn1_5Gadm.tsv").toString()
  def antarcticaInputTsvPath = getClass.getResource("/antarctica.tsv").toString()

  def Dashboard(features: RDD[ValidatedLocation[Geometry]], doGadmIntersect: Boolean) = {
    val fireAlertsRdd = {
      // I guess this is how they do things in Java?
      val spatialRDD = new SpatialRDD[Geometry]
      spatialRDD.rawSpatialRDD = spark.sparkContext.emptyRDD[Geometry].toJavaRDD()
      spatialRDD
    }

    GfwProDashboardAnalysis(
      features,
      "gfwpro",
      doGadmIntersect,
      NonEmptyList.one(idn1_5GadmTsvPath),
      fireAlertsRdd,
      spark,
      kwargs = Map(
        "config" -> GfwConfig.get(Some(NonEmptyList.one(Config("gfw_integrated_alerts", "v20231121")))),
        "gadmVers" -> "3.6"
      ),
    )
  }

  /** Function to update expected results when this test becomes invalid */
  def saveExpectedFcdResult(fcd: DataFrame): Unit = {
    fcd.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .options(GfwProDashboardExport.csvOptions)
      .csv(path = dashExpectedOutputPath)
  }

  def readExpectedFcdResult = {
    val csvFile = spark.read
      .options(GfwProDashboardExport.csvOptions)
      .csv(dashExpectedOutputPath)
    // status_code and glad_alerts_coverage get interpreted as string type, so cast
    // them to their correct integer/boolean type.
    csvFile.withColumn("status_code", col("status_code").cast(IntegerType))
      .withColumn("glad_alerts_coverage", col("glad_alerts_coverage").cast(BooleanType))
      .withColumn("integrated_alerts_coverage", col("integrated_alerts_coverage").cast(BooleanType))
  }

  it("matches recorded output for dashboard for vector gadm", ProTag) {
    val featureLoc31RDD = ValidatedFeatureRDD(
      NonEmptyList.one(dashInputTsvPath),
      "gfwpro",
      FeatureFilter.empty,
      splitFeatures = true,
      gfwProAddCentroid = true
    )
    val fcd = Dashboard(featureLoc31RDD, true)
    val summaryDF = GfwProDashboardDF.getFeatureDataFrameFromVerifiedRdd(fcd.unify, spark)
    summaryDF.collect().foreach(println)
    //saveExpectedFcdResult(summaryDF)

    val expectedDF = readExpectedFcdResult

    assertSmallDataFrameEquality(summaryDF, expectedDF)
  }

  it("matches recorded output for dashboard for raster gadm", ProTag) {
    val featureLoc31RDD = ValidatedFeatureRDD(
      NonEmptyList.one(dashInputTsvPath),
      "gfwpro",
      FeatureFilter.empty,
      splitFeatures = true,
      gfwProAddCentroid = true
    )
    val fcd = Dashboard(featureLoc31RDD, false)
    val summaryDF = GfwProDashboardDF.getFeatureDataFrameFromVerifiedRdd(fcd.unify, spark)
    summaryDF.collect().foreach(println)
    //saveExpectedFcdResult(summaryDF)

    val expectedDF = readExpectedFcdResult

    assertSmallDataFrameEquality(summaryDF, expectedDF)
  }

  it("completes without error for list that doesn't intersect TCL at all") {
    // The antarctica location will be completely removed by splitFeatures, since it
    // doesn't intersect with the TCL extent (which is used as the splitting grid).
    val antRDD = ValidatedFeatureRDD(
      NonEmptyList.one(antarcticaInputTsvPath),
      "gfwpro",
      FeatureFilter.empty,
      splitFeatures = true
    )
    val fcd = Dashboard(antRDD, true)
    val summaryDF = GfwProDashboardDF.getFeatureDataFrameFromVerifiedRdd(fcd.unify, spark)

    summaryDF.count() shouldBe 0
  }

}
