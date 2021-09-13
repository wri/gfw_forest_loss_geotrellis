package org.globalforestwatch.features

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.globalforestwatch.util.Util._
import org.globalforestwatch.summarystats.SummaryCommand
import org.apache.spark.sql.Column

object SimpleFeature extends Feature {

  val idPos = 0
  val geomPos = 1
  val featureCount = 1

  val featureIdExpr = "cast(fid as int) as featureId"


  def getFeatureId(i: Array[String], parsed: Boolean = false): FeatureId = {
    val feature_id: Int = i(idPos).toInt
    SimpleFeatureId(feature_id)
  }

  case class Filter(
    base: Option[SummaryCommand.BaseFilter],
    id: Option[SummaryCommand.FeatureIdFilter]
  ) extends FeatureFilter {
    def filterConditions: List[Column]= {
      base.toList.flatMap(_.filters()) ++
        id.toList.flatMap(_.filters(idColumn=col("fid")))
    }
  }
}
