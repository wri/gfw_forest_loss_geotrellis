package org.globalforestwatch.features

import cats.data.NonEmptyList
import org.locationtech.jts
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, isnull, udf}
import org.globalforestwatch.util.GeotrellisGeometryReducer.{gpr, reduce}
import org.globalforestwatch.util.GeotrellisGeometryValidator.preserveGeometryType
import org.locationtech.jts.geom.util.GeometryFixer
import org.locationtech.jts.geom.{Geometry, MultiPolygon, Polygon}

import scala.util.Try

object SpatialFeatureDF {
  def apply(input: NonEmptyList[String],
            featureObj: Feature,
            filters: FeatureFilter,
            spark: SparkSession,
            lonField: String,
            latField: String): DataFrame = {
    val df: DataFrame = FeatureDF(input, featureObj, filters, spark)

    val viewName = featureObj.getClass.getSimpleName.dropRight(1).toLowerCase
    df.createOrReplaceTempView(viewName)
    val spatialDf = spark.sql(
      s"""
         |SELECT ST_Point(CAST($lonField AS Decimal(24,10)),CAST($latField AS Decimal(24,10))) AS pointshape, *
         |FROM $viewName
      """.stripMargin)

    spatialDf
  }

  /*
   * Use GeoSpark to directly generate a DataFrame with a geometry column
   */
  def apply(input: NonEmptyList[String],
            featureType: String,
            filters: FeatureFilter,
            wkbField: String,
            spark: SparkSession,
            delimiter: String = "\t"): DataFrame = {

    val featureObj: Feature = Feature(featureType)
    SpatialFeatureDF(input, featureObj, filters, wkbField, spark, delimiter)
  }

  def apply(input: NonEmptyList[String],
            featureObj: Feature,
            filters: FeatureFilter,
            wkbField: String,
            spark: SparkSession,
            delimiter: String): DataFrame = {

    val featureDF: DataFrame =
      FeatureDF(input, featureObj, filters, spark, delimiter)
    val emptyPolygonWKB = "0106000020E610000000000000"

    val readOptionWkbUDF = udf {
      s: String =>
        val geom: Option[Geometry] = readOption(s)

        geom match {
          case Some(g) =>
            Some(preserveGeometryType(GeometryFixer.fix(g), g.getGeometryType))
          case None => None
        }
    }

    featureDF
      .where(s"${wkbField} != '${emptyPolygonWKB}'")
      .selectExpr(
        s"${wkbField} AS wkb",
        s"struct(${featureObj.featureIdExpr}) as featureId"
      )
      .select(
        readOptionWkbUDF (col("wkb")).as("polyshape"),
        col("featureId")
      )
      .where(!isnull(col("polyshape")))
  }

  /*
   * Use GeoSpark to directly generate a DataFrame with a geometry column
   * Any geometry that fails to be parsed as WKB will be dropped here
   */
  def applyValidated(
    input: NonEmptyList[String],
    featureObj: Feature,
    filters: FeatureFilter,
    wkbField: String,
    spark: SparkSession,
    delimiter: String = "\t"
  ): DataFrame = {
    import spark.implicits._

    val featureDF: DataFrame = FeatureDF(input, featureObj, filters, spark, delimiter)
    val emptyPolygonWKB = "0106000020E610000000000000"
    val readOptionWkbUDF = udf {
      s: String =>
        val geom: Option[Geometry] = readOption(s)

        geom match {
          case Some(g) =>
            Some(preserveGeometryType(GeometryFixer.fix(g), g.getGeometryType))
          case None => None
        }
    }

    featureDF
      .where(s"${wkbField} != '${emptyPolygonWKB}'")
      .selectExpr(
        s"${wkbField} AS wkb",
        s"struct(${featureObj.featureIdExpr}) as featureId"
      )
      .select(
        readOptionWkbUDF (col("wkb")).as("polyshape"),
        col("featureId")
      )
      .where(!isnull('polyshape))
  }

  private val threadLocalWkbReader = new ThreadLocal[jts.io.WKBReader]

  def readOption(wkbHexString: String): Option[jts.geom.Geometry] = {
    if (threadLocalWkbReader.get() == null) {
      val precisionModel = new jts.geom.PrecisionModel(1e11)
      val factory = new jts.geom.GeometryFactory(precisionModel)
      val wkbReader = new jts.io.WKBReader(factory)
      threadLocalWkbReader.set(wkbReader)
    }
    val wkbReader = threadLocalWkbReader.get()

    Try{
      val binWkb = javax.xml.bind.DatatypeConverter.parseHexBinary(wkbHexString)
      wkbReader.read(binWkb)
    }.toOption
  }
}
