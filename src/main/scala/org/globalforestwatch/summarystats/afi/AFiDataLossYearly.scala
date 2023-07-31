package org.globalforestwatch.summarystats.afi

import cats.implicits._
import cats.kernel.Semigroup
import frameless.Injection
import io.circe.syntax._
import scala.collection.immutable.SortedMap
import io.circe.syntax._
import io.circe.parser.decode
import cats.kernel.Semigroup
import cats.implicits._

import scala.collection.immutable.SortedMap

case class AFiDataLossYearly(value: SortedMap[Int, Double])
  extends AFiDataParser[AFiDataLossYearly] {

  def merge(other: AFiDataLossYearly): AFiDataLossYearly = {
    AFiDataLossYearly(Semigroup[SortedMap[Int, Double]].combine(value, other.value))
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

  def prefilled: AFiDataLossYearly =
    AFiDataLossYearly(
      SortedMap(
        2001 -> 0,
        2002 -> 0,
        2003 -> 0,
        2004 -> 0,
        2005 -> 0,
        2006 -> 0,
        2007 -> 0,
        2008 -> 0,
        2009 -> 0,
        2010 -> 0,
        2011 -> 0,
        2012 -> 0,
        2013 -> 0,
        2014 -> 0,
        2015 -> 0,
        2016 -> 0,
        2017 -> 0,
        2018 -> 0,
        2019 -> 0,
        2020 -> 0,
        2021 -> 0,
      )
    )

  def fill(lossYear: Int,
           areaHa: Double,
           include: Boolean = true): AFiDataLossYearly = {

    // Only except lossYear values within range of default map
    val minLossYear: Int = this.prefilled.value.keysIterator.min
    val maxLossYear: Int = this.prefilled.value.keysIterator.max

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


