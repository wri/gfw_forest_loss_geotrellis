package org.globalforestwatch.treecoverloss

import scala.collection.mutable.Map
import play.api.libs.json._


case class LossYearData(year: Int,
                        var area_loss: Double,
                        var biomass_loss: Double,
                        var carbon_emissions: Double,
                        var mangrove_biomass_loss: Double,
                        var mangrove_carbon_emissions: Double)

object LossYearDataMap {
  def empty: Map[Int, LossYearData] = Map(
    2001 -> LossYearData(2001, 0, 0, 0, 0, 0),
    2002 -> LossYearData(2002, 0, 0, 0, 0, 0),
    2003 -> LossYearData(2003, 0, 0, 0, 0, 0),
    2004 -> LossYearData(2004, 0, 0, 0, 0, 0),
    2005 -> LossYearData(2005, 0, 0, 0, 0, 0),
    2006 -> LossYearData(2006, 0, 0, 0, 0, 0),
    2007 -> LossYearData(2007, 0, 0, 0, 0, 0),
    2008 -> LossYearData(2008, 0, 0, 0, 0, 0),
    2009 -> LossYearData(2009, 0, 0, 0, 0, 0),
    2010 -> LossYearData(2010, 0, 0, 0, 0, 0),
    2011 -> LossYearData(2011, 0, 0, 0, 0, 0),
    2012 -> LossYearData(2012, 0, 0, 0, 0, 0),
    2013 -> LossYearData(2013, 0, 0, 0, 0, 0),
    2014 -> LossYearData(2014, 0, 0, 0, 0, 0),
    2015 -> LossYearData(2015, 0, 0, 0, 0, 0),
    2016 -> LossYearData(2016, 0, 0, 0, 0, 0),
    2017 -> LossYearData(2017, 0, 0, 0, 0, 0),
    2018 -> LossYearData(2018, 0, 0, 0, 0, 0))

  //def mapValuesToList(map: Map[Int, LossYearData]): List[LossYearData] = ???

  def toList(map: Map[Int, LossYearData]): List[LossYearData] = map.values.toList

  def toJson(map: Map[Int, LossYearData]): JsObject = {
    implicit val lossYearFormat = Json.format[LossYearData]
    val list = map.values
    Json.obj("year_data" -> list)
  }

}

//
//object LossYearData {
//  def empty
//    : scala.collection.mutable.Map[Int,
//                                   (Double, Double, Double, Double, Double)] =
//    scala.collection.mutable.Map(
//      2001 -> (0.0, 0.0, 0.0, 0.0, 0.0),
//      2002 -> (0.0, 0.0, 0.0, 0.0, 0.0),
//      2003 -> (0.0, 0.0, 0.0, 0.0, 0.0),
//      2004 -> (0.0, 0.0, 0.0, 0.0, 0.0),
//      2005 -> (0.0, 0.0, 0.0, 0.0, 0.0),
//      2006 -> (0.0, 0.0, 0.0, 0.0, 0.0),
//      2007 -> (0.0, 0.0, 0.0, 0.0, 0.0),
//      2008 -> (0.0, 0.0, 0.0, 0.0, 0.0),
//      2009 -> (0.0, 0.0, 0.0, 0.0, 0.0),
//      2010 -> (0.0, 0.0, 0.0, 0.0, 0.0),
//      2011 -> (0.0, 0.0, 0.0, 0.0, 0.0),
//      2012 -> (0.0, 0.0, 0.0, 0.0, 0.0),
//      2013 -> (0.0, 0.0, 0.0, 0.0, 0.0),
//      2014 -> (0.0, 0.0, 0.0, 0.0, 0.0),
//      2015 -> (0.0, 0.0, 0.0, 0.0, 0.0),
//      2016 -> (0.0, 0.0, 0.0, 0.0, 0.0),
//      2017 -> (0.0, 0.0, 0.0, 0.0, 0.0),
//      2018 -> (0.0, 0.0, 0.0, 0.0, 0.0)
//    )
//
//  def toString(
//    myMap: scala.collection.mutable.Map[
//      Int,
//      (Double, Double, Double, Double, Double)
//    ]
//  ): String = {
//
//    var mapString = "["
//    for ((k, v) <- myMap) {
//
//      mapString += s"""{\"year\": $k, \"area_loss\": ${v._1}, \"biomass_loss\": ${v._2}, \"carbon_emissions\":${v._3},  \"mangrove_biomass_loss\": ${v._4}, \"mangrove_carbon_emissions\": ${v._5}},"""
//    }
//    mapString = mapString.dropRight(1) + "]"
//
//    mapString
//  }
//}
