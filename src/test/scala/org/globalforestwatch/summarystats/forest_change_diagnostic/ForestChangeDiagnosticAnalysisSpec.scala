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
import org.globalforestwatch.TestEnvironment
import org.globalforestwatch.config.GfwConfig

class ForestChangeDiagnosticAnalysisSpec extends TestEnvironment with DataFrameComparer {
  def palm32InputTsvPath = getClass.getResource("/palm-oil-32.tsv").toString()
  def palm32ExpectedOutputPath = getClass.getResource("/palm-32-fcd-output").toString()

  def FCD(features: RDD[ValidatedLocation[Geometry]]) = {
    val fireAlertsRdd = {
      // I guess this is how they do things in Java?
      val spatialRDD = new SpatialRDD[Geometry]
      spatialRDD.rawSpatialRDD = spark.sparkContext.emptyRDD[Geometry].toJavaRDD()
      spatialRDD
    }

    ForestChangeDiagnosticAnalysis(
      features,
      intermediateResultsRDD = None,
      fireAlerts = fireAlertsRdd,
      saveIntermidateResults = identity,
      kwargs = Map("config" -> GfwConfig.get))
  }

  /** Function to update expected results when this test becomes invalid */
  def saveExpectedFcdResult(fcd: DataFrame): Unit = {
    fcd
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .options(ForestChangeDiagnosticExport.csvOptions)
      .csv(path = palm32ExpectedOutputPath)
  }

  def readExpectedFcdResult = {
    spark.read
      .options(ForestChangeDiagnosticExport.csvOptions)
      .csv(palm32ExpectedOutputPath)
      .withColumn("status_code", col("status_code").cast(IntegerType))
  }

  it("matches recorded output for palm oil mills location 31") {
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
    // saveExpectedFcdResult(fcdDF)

    val expectedDF = readExpectedFcdResult

    assertSmallDataFrameEquality(fcdDF, expectedDF)
  }

  it("reports invalid geoms") {
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

  it("gives results for geometries with tiles that don't intersect") {
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

  it("reports no intersection geometries as invalid") {
    // match it to tile of location 31 so we fetch less tiles
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
}
