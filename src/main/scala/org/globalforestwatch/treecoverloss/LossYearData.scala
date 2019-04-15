package org.globalforestwatch.treecoverloss

object LossYearData {
  def empty
    : scala.collection.mutable.Map[Int,
                                   (Double, Double, Double, Double, Double)] =
    scala.collection.mutable.Map(
      2001 -> (0.0, 0.0, 0.0, 0.0, 0.0),
      2002 -> (0.0, 0.0, 0.0, 0.0, 0.0),
      2003 -> (0.0, 0.0, 0.0, 0.0, 0.0),
      2004 -> (0.0, 0.0, 0.0, 0.0, 0.0),
      2005 -> (0.0, 0.0, 0.0, 0.0, 0.0),
      2006 -> (0.0, 0.0, 0.0, 0.0, 0.0),
      2007 -> (0.0, 0.0, 0.0, 0.0, 0.0),
      2008 -> (0.0, 0.0, 0.0, 0.0, 0.0),
      2009 -> (0.0, 0.0, 0.0, 0.0, 0.0),
      2010 -> (0.0, 0.0, 0.0, 0.0, 0.0),
      2011 -> (0.0, 0.0, 0.0, 0.0, 0.0),
      2012 -> (0.0, 0.0, 0.0, 0.0, 0.0),
      2013 -> (0.0, 0.0, 0.0, 0.0, 0.0),
      2014 -> (0.0, 0.0, 0.0, 0.0, 0.0),
      2015 -> (0.0, 0.0, 0.0, 0.0, 0.0),
      2016 -> (0.0, 0.0, 0.0, 0.0, 0.0),
      2017 -> (0.0, 0.0, 0.0, 0.0, 0.0),
      2018 -> (0.0, 0.0, 0.0, 0.0, 0.0)
    )

  def toString(
    myMap: scala.collection.mutable.Map[
      Int,
      (Double, Double, Double, Double, Double)
    ]
  ): String = {

    var mapString = "["
    for ((k, v) <- myMap) {

      mapString += s"""{"year_": $k, "area_loss": ${v._1}, "biomass_loss": ${v._2}, "carbon_emissions":${v._3},  "mangrove_biomass_loss": ${v._4}, "mangrove_carbon_emissions": ${v._5}},"""
    }
    mapString = mapString.dropRight(1) + "]"

    mapString
  }
}
