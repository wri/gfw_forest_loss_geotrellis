package org.globalforestwatch.summarystats.afi

import frameless.Injection

import scala.collection.immutable.SortedMap
import io.circe.syntax._
import io.circe.parser.decode
import cats.kernel.Semigroup
import cats.implicits._
import org.globalforestwatch.summarystats.forest_change_diagnostic.ForestChangeDiagnosticDataParser

case class AFiDataLossYearly(value: SortedMap[Int, Double])
  extends ForestChangeDiagnosticDataParser[AFiDataLossYearly] {

  def merge(other: AFiDataLossYearly): AFiDataLossYearly = {
    val r = AFiDataLossYearly(Semigroup[SortedMap[Int, Double]].combine(value, other.value))
    r
  }

  def round: SortedMap[Int, Double] = this.value.map { case (key, value) => key -> this.round(value) }

  def limitToMaxYear(maxYear: Int): AFiDataLossYearly = {
    AFiDataLossYearly(value.filterKeys{ year => year <= maxYear })
  }

  def toJson: String = {
    this.round.asJson.noSpaces
  }
}

object AFiDataLossYearly {
  def empty: AFiDataLossYearly =
    AFiDataLossYearly(
      SortedMap()
    )

  def prefilled: AFiDataLossYearly = {
    val minLossYear = 2020
    val maxLossYear = 2023
    val kvList = for (i <- minLossYear to maxLossYear) yield(i -> 0.0)
    AFiDataLossYearly(
      SortedMap(kvList.toSeq: _*)
    )
  }

  def fill(lossYear: Int,
           areaHa: Double,
           include: Boolean = true): AFiDataLossYearly = {

    // Only accept lossYear values within range of default map
    val minLossYear = 2020
    val maxLossYear = 2023

    if (minLossYear <= lossYear && lossYear <= maxLossYear && include) {
      AFiDataLossYearly.prefilled.merge(
        AFiDataLossYearly(
          SortedMap(
            lossYear -> areaHa
          )
        )
      )
    } else
      this.empty
  }

  def fromString(value: String): AFiDataLossYearly = {
    val sortedMap = decode[SortedMap[Int, Double]](value)
    AFiDataLossYearly(sortedMap.getOrElse(SortedMap()))
  }

  implicit def injection: Injection[AFiDataLossYearly, String] = Injection(_.toJson, fromString)
}
