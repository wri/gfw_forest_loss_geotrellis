package org.globalforestwatch.features

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.globalforestwatch.util.Util._

object GfwProFeature extends Feature {

  val listIdPos = 0
  val locationIdPos = 1
  val geomPos = 2

  val featureIdExpr = "list_id as listId, cast(location_id as int) as locationId"


  def getFeatureId(i: Array[String], parsed: Boolean = false): FeatureId = {

    val listId: String = i(listIdPos)
    val locationId: Int = i(locationIdPos).toInt
    GfwProFeatureId(listId, locationId)
  }

  override def custom_filter(
                              filters: Map[String, Any]
                            )(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession
    import spark.implicits._

    val idStart: Option[Int] = getAnyMapValue[Option[Int]](filters, "idStart")
    //    val idEnd: Option[Int] = getAnyMapValue[Option[Int]](filters, "idEnd")

    //    val idStartDF: DataFrame =
    idStart.foldLeft(df)((acc, i) => acc.filter($"locationId" >= i))

    //    idEnd.foldLeft(idStartDF)((acc, i) => acc.filter($"fid" < i))

  }
}
