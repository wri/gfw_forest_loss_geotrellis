package org.globalforestwatch.summarystats.forest_change_diagnostic

import cats.data.{NonEmptyList, Validated}
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import geotrellis.vector._
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.globalforestwatch.features.{FeatureFilter, ValidatedFeatureRDD}
import org.globalforestwatch.features.GfwProFeatureId
import org.globalforestwatch.summarystats.{JobError, Location, ValidatedLocation}
import org.globalforestwatch.{TestEnvironment, ProTag}
import org.globalforestwatch.config.GfwConfig
import org.globalforestwatch.layers.ApproxYear

class ForestChangeDiagnosticAnalysisSpec extends TestEnvironment with DataFrameComparer {
  def palm32InputTsvPath = getClass.getResource("/palm-oil-32.tsv").toString()
  def antarcticaInputTsvPath = getClass.getResource("/antarctica.tsv").toString()
  def argentinaInputTsvPath = getClass.getResource("/argentina.tsv").toString()
  def argBraInputTsvPath = getClass.getResource("/argbra.tsv").toString()
  def colInputTsvPath = getClass.getResource("/colombia.tsv").toString()
  def palm32ExpectedOutputPath = getClass.getResource("/palm-32-fcd-output").toString()
  def argBraExpectedOutputPath = getClass.getResource("/argbra-fcd-output").toString()
  def colExpectedOutputPath = getClass.getResource("/col-fcd-output").toString()

  def FCD(features: RDD[ValidatedLocation[Geometry]]) = {
    val fireAlertsRdd = {
      // I guess this is how they do things in Java?
      val spatialRDD = new SpatialRDD[Geometry]
      spatialRDD.rawSpatialRDD = spark.sparkContext.emptyRDD[Geometry].toJavaRDD()
      spatialRDD
    }

    ForestChangeDiagnosticAnalysis(
      features,
      fireAlerts = fireAlertsRdd,
      kwargs = Map("config" -> GfwConfig.get()))
  }

  /** Function to update expected results when this test becomes invalid */
  def saveExpectedFcdResult(fcd: DataFrame, outputPath: String): Unit = {
    fcd.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .options(ForestChangeDiagnosticExport.csvOptions)
      .csv(path = outputPath)
  }

  def readExpectedFcdResult(path: String) = {
    spark.read
      .options(ForestChangeDiagnosticExport.csvOptions)
      .csv(path)
      .withColumn("status_code", col("status_code").cast(IntegerType))
  }

  it("matches recorded output for palm oil mills location 31", ProTag) {
    val featureLoc31RDD = ValidatedFeatureRDD(
      NonEmptyList.one(palm32InputTsvPath),
      "gfwpro",
      FeatureFilter.empty,
      splitFeatures = true
    ).filter {
      case Validated.Valid(Location(GfwProFeatureId(_, 31), _)) => true
      case _ => false
    }
    val fcd = FCD(featureLoc31RDD)
    val fcdDF = ForestChangeDiagnosticDF.getFeatureDataFrame(fcd, spark)
    // saveExpectedFcdResult(fcdDF, palm32ExpectedOutputPath)

    val expectedDF = readExpectedFcdResult(palm32ExpectedOutputPath)

    assertSmallDataFrameEquality(fcdDF, expectedDF)
  }

  it("reports self-intersecting geometry as invalid") {
    // match it to tile of location 31 so we fetch less tiles
    val x = 114
    val y = -1
    val selfIntersectingPolygon = Polygon(LineString(Point(x+0,y+0), Point(x+1,y+0), Point(x+0,y+1), Point(x+1,y+1), Point(x+0,y+0)))
    val featureRDD = spark.sparkContext.parallelize(List(
      Validated.valid[Location[JobError], Location[Geometry]](Location(GfwProFeatureId("1", 1), selfIntersectingPolygon))
    ))
    val fcd = FCD(featureRDD)
    val res = fcd.collect()
    res.head.isInvalid shouldBe true
  }

  it("gives results for geometries with multiple polygons that don't intersect", ProTag) {
    val featureLoc32RDD = ValidatedFeatureRDD(
      NonEmptyList.one(palm32InputTsvPath),
      "gfwpro",
      FeatureFilter.empty,
      splitFeatures = true
    ).filter {
      case Validated.Valid(Location(GfwProFeatureId(_, 32), _)) => true
      case _ => false
    }
    val fcd = FCD(featureLoc32RDD)
    val res = fcd.collect()
    res.head.isValid shouldBe true
  }

  it("reports no-intersection geometry as invalid") {
    // This still works, since we are not calling ValidatedFeatureRDD/splitFeatures here.
    val x = 60
    val y = 20
    val smallGeom = Polygon(LineString(Point(x+0,y+0), Point(x+0.00001,y+0), Point(x+0.00001,y+0.00001), Point(x+0,y+0.00001), Point(x+0,y+0)))
    val featureRDD = spark.sparkContext.parallelize(List(
      Validated.valid[Location[JobError], Location[Geometry]](Location(GfwProFeatureId("1", 1), smallGeom))
    ))
    val fcd = FCD(featureRDD)
    val res = fcd.collect()
    res.head.isInvalid shouldBe true
  }

  it("completes without error for list that doesn't intersect TCL at all", ProTag) {
    // The antarctica location will be completely removed by splitFeatures, since it
    // doesn't intersect with the TCL extent (which is used as the splitting grid).
    val antRDD = ValidatedFeatureRDD(
      NonEmptyList.one(antarcticaInputTsvPath),
      "gfwpro",
      FeatureFilter.empty,
      splitFeatures = true
    )
    val fcd = FCD(antRDD)
    val fcdDF = ForestChangeDiagnosticDF.getFeatureDataFrame(fcd, spark)

    fcdDF.count() shouldBe 0
  }

  it("gives an approx loss year for Argentina location", ProTag) {
    val argentinaRDD = ValidatedFeatureRDD(
      NonEmptyList.one(argentinaInputTsvPath),
      "gfwpro",
      FeatureFilter.empty,
      splitFeatures = true
    )
    val fcd = FCD(argentinaRDD)
    val loc: ValidatedLocation[ForestChangeDiagnosticData] = fcd.collect().head
    loc.isValid shouldBe true
    val fcdd: ForestChangeDiagnosticData = loc.getOrElse(null)._2
    (fcdd.country_specific_deforestation_yearly.value("ARG").value(ApproxYear(2017, true)) > 0) shouldBe true

    val fcdDF = ForestChangeDiagnosticDF.getFeatureDataFrame(fcd, spark)
    val tcl: String = fcdDF.collect().head.getAs[String]("country_specific_deforestation_yearly")
    tcl.contains("2017approx") shouldBe true
  }

  it("matches recorded output for location including ARG and BRA", ProTag) {
    val argBraRDD = ValidatedFeatureRDD(
      NonEmptyList.one(argBraInputTsvPath),
      "gfwpro",
      FeatureFilter.empty,
      splitFeatures = true
    )
    val fcd = FCD(argBraRDD)
    val fcdDF = ForestChangeDiagnosticDF.getFeatureDataFrame(fcd, spark)
    // saveExpectedFcdResult(fcdDF, argBraExpectedOutputPath)

    val expectedDF = readExpectedFcdResult(argBraExpectedOutputPath)

    assertSmallDataFrameEquality(fcdDF, expectedDF)
  }

  it("matches recorded output for COL location", ProTag) {
    val colRDD = ValidatedFeatureRDD(
      NonEmptyList.one(colInputTsvPath),
      "gfwpro",
      FeatureFilter.empty,
      splitFeatures = true
    )
    val fcd = FCD(colRDD)
    val fcdDF = ForestChangeDiagnosticDF.getFeatureDataFrame(fcd, spark)
    //saveExpectedFcdResult(fcdDF, colExpectedOutputPath)

    val expectedDF = readExpectedFcdResult(colExpectedOutputPath)

    assertSmallDataFrameEquality(fcdDF, expectedDF)
  }
}
