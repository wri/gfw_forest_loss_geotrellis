package org.globalforestwatch.summarystats.afi

import frameless.Injection
import io.circe.syntax._
import io.circe.parser.decode


case class AFiDataLossYearlyCategory(
                                       value: Map[String, AFiDataLossYearly]
                                    ) extends AFiDataParser[
  AFiDataLossYearlyCategory
] {
  def merge(
             other: AFiDataLossYearlyCategory
           ): AFiDataLossYearlyCategory = {

    AFiDataLossYearlyCategory(value ++ other.value.map {
      case (key, otherValue) =>
        key -> value
          .getOrElse(key, AFiDataLossYearly.empty)
          .merge(otherValue)
    })
  }

  def toJson: String = {
    this.value
      .map {
        case (key, value) =>
          key -> value.round
      }
      .asJson
      .noSpaces
  }
}

object AFiDataLossYearlyCategory {
  def empty: AFiDataLossYearlyCategory =
    AFiDataLossYearlyCategory(Map())

  def fill(
            className: String,
            lossYear: Int,
            areaHa: Double,
            noData: List[String] = List("", "Unknown", "Not applicable"),
            include: Boolean = true
          ): AFiDataLossYearlyCategory = {

    if (noData.contains(className))
      AFiDataLossYearlyCategory.empty
    else
      AFiDataLossYearlyCategory(
        Map(
          className -> AFiDataLossYearly
            .fill(lossYear, areaHa, include)
        )
      )
  }

  def fromString(
                  value: String
                ): AFiDataLossYearlyCategory = {

    val categories: Map[String, String] =
      decode[Map[String, String]](value).getOrElse(Map())
    val newValues = categories.map {
      case (k, v) => (k, AFiDataLossYearly.fromString(v))
    }

    AFiDataLossYearlyCategory(newValues)

  }

  implicit def injection: Injection[AFiDataLossYearlyCategory, String] = Injection(_.toJson, fromString)
}
