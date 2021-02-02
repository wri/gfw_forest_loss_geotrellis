package org.globalforestwatch.summarystats.forest_change_diagnostic

import org.globalforestwatch.util.Implicits._
import scala.collection.immutable.SortedMap

case class ForestChangeDiagnosticTCLYearly(stats: SortedMap[Int, Double]) {
  def merge(
    other: ForestChangeDiagnosticTCLYearly
  ): ForestChangeDiagnosticTCLYearly = {
    ForestChangeDiagnosticTCLYearly(stats ++ other.stats.map {
      case (key, otherValue) =>
        key ->
          (stats(key) + otherValue)

    })
  }
}

object ForestChangeDiagnosticTCLYearly {
  def empty: ForestChangeDiagnosticTCLYearly =
    ForestChangeDiagnosticTCLYearly(
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
        2019 -> 0
      )
    )

  def fill(lossYear: Int, areaHa: Double, include: Boolean = true): ForestChangeDiagnosticTCLYearly = {
    ForestChangeDiagnosticTCLYearly(SortedMap(
      lossYear ->
        areaHa * include
    ))
  }

}
