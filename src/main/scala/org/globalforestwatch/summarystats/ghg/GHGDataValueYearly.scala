package org.globalforestwatch.summarystats.ghg

import frameless.Injection

import scala.collection.immutable.SortedMap
import io.circe.syntax._
import io.circe.parser.decode
import cats.kernel.Semigroup
import cats.implicits._

case class GHGDataValueYearly(value: SortedMap[Int, Double])
  extends GHGDataParser[GHGDataValueYearly] {

  def merge(other: GHGDataValueYearly): GHGDataValueYearly = {
    GHGDataValueYearly(Semigroup[SortedMap[Int, Double]].combine(value, other.value))
  }

  def round: SortedMap[Int, Double] = this.value.map { case (key, value) => key -> this.round(value) }

  def toJson: String = {
    this.round.asJson.noSpaces
  }
}

object GHGDataValueYearly {
  def empty: GHGDataValueYearly =
    GHGDataValueYearly(
      SortedMap()
    )

  def fromString(value: String): GHGDataValueYearly = {
    val sortedMap = decode[SortedMap[Int, Double]](value)
    GHGDataValueYearly(sortedMap.getOrElse(SortedMap()))
  }

  implicit def injection: Injection[GHGDataValueYearly, String] = Injection(_.toJson, fromString)
}
