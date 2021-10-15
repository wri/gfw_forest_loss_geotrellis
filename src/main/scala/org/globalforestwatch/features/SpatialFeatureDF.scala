package org.globalforestwatch.features

import cats.data.NonEmptyList
import geotrellis.vector.Geometry
import com.vividsolutions.{jts => vs}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, isnull, struct, udf}
import org.globalforestwatch.util.GeometryFixer
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

    // ST_PrecisionReduce may create invalid geometry if it contains a "sliver" that is below the precision threshold
    // ST_Buffer(0) fixes these invalid geometries
    featureDF
     .selectExpr(
       s"ST_Buffer(ST_PrecisionReduce(ST_GeomFromWKB(${wkbField}), 11), 0) AS polyshape",
       s"struct(${featureObj.featureIdExpr}) as featureId"
     )
     .where(s"${wkbField} != '${emptyPolygonWKB}'")
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
    val readOptionWkbUDF = udf{ s: String => readOption(s) }
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

  private val threadLocalWkbReader = new ThreadLocal[vs.io.WKBReader]

  def readOption(wkbHexString: String): Option[vs.geom.Geometry] = {
    if (threadLocalWkbReader.get() == null) {
      val precisionModel = new vs.geom.PrecisionModel(1e11)
      val factory = new vs.geom.GeometryFactory(precisionModel)
      val wkbReader = new vs.io.WKBReader(factory)
      threadLocalWkbReader.set(wkbReader)
    }
    val wkbReader = threadLocalWkbReader.get()

    Try{
      val binWkb = javax.xml.bind.DatatypeConverter.parseHexBinary(wkbHexString)
      wkbReader.read(binWkb)
    }.toOption
  }
}
