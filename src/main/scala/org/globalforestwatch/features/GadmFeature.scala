package org.globalforestwatch.features

import org.apache.spark.sql.Column
import org.globalforestwatch.summarystats.SummaryCommand

object GadmFeature extends Feature {
  val countryPos = 1
  val adm1Pos = 2
  val adm2Pos = 3
  val geomPos = 7

  val featureIdExpr =
    "gid_0 as iso, split(split(gid_1, '\\\\.')[1], '_')[0] as adm1, split(split(gid_2, '\\\\.')[2], '_')[0] as adm2"


  def getFeatureId(i: Array[String], parsed: Boolean = false): FeatureId = {
    if (parsed) {
      val countryCode = i(0)
      val admin1: Integer = try {
        i(1).toInt
      } catch {
        case e: Exception => null
      }

      val admin2: Integer = try {
        i(2).toInt
      } catch {
        case e: Exception => null
      }

      GadmFeatureId(countryCode, admin1, admin2)
    } else {
      val countryCode = i(countryPos)
      val admin1: Integer = try {
        i(adm1Pos).split("[.]")(1).split("[_]")(0).toInt
      } catch {
        case e: Exception => null
      }

      val admin2: Integer = try {
        i(adm2Pos).split("[.]")(2).split("[_]")(0).toInt
      } catch {
        case e: Exception => null
      }

      GadmFeatureId(countryCode, admin1, admin2)
    }
  }

  case class Filter(
    base: Option[SummaryCommand.BaseFilter],
    gadm: Option[SummaryCommand.GadmFilter]
  ) extends FeatureFilter {
    def filterConditions(): List[Column]= {
      import org.apache.spark.sql.functions.col
      base.toList.flatMap(_.filters()) ++
        gadm.toList.flatMap(_.filters(isoColumn = col("gid_0"), admin1Column = col("gid_1"), admin2Column = col("gid_2"))).toList
    }
  }
}
