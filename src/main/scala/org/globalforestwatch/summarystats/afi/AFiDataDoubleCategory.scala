package org.globalforestwatch.summarystats.afi

import frameless.Injection
import io.circe.syntax._
import io.circe.parser.decode

case class AFiDataDoubleCategory(value: Map[String, AFiDataDouble]) extends AFiDataParser[AFiDataDoubleCategory] {
  def merge(
    other: AFiDataDoubleCategory
  ): AFiDataDoubleCategory = {

    AFiDataDoubleCategory(value ++ other.value.map {
      case (key, otherValue) =>
        key -> value
          .getOrElse(key, AFiDataDouble.empty)
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

object AFiDataDoubleCategory {
  def empty: AFiDataDoubleCategory =
    AFiDataDoubleCategory(Map())

  def fill(
            className: String,
            areaHa: Double,
            noData: List[String] = List("", "Unknown", "Not applicable"),
            include: Boolean = true
          ): AFiDataDoubleCategory = {
    if (noData.contains(className))
      AFiDataDoubleCategory.empty
    else
      AFiDataDoubleCategory(
        Map(className -> AFiDataDouble.fill(areaHa, include))
      )
  }

  def fromString(value: String): AFiDataDoubleCategory = {

    val categories: Map[String, String] = decode[Map[String, String]](value).getOrElse(Map())
    val newValue: Map[String, AFiDataDouble] = categories.map { case (k, v) => (k, AFiDataDouble(v.toDouble)) }
    AFiDataDoubleCategory(newValue)

  }

  implicit def injection: Injection[AFiDataDoubleCategory , String] = Injection(_.toJson, fromString)
}
