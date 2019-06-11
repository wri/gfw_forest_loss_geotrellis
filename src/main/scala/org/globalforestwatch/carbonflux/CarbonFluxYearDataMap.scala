package org.globalforestwatch.carbonflux

import scala.collection.mutable.Map

object CarbonFluxYearDataMap {
  def empty: Map[Int, CarbonFluxYearData] =
    Map(
      2001 -> CarbonFluxYearData(2001, 0, 0, 0),
      2002 -> CarbonFluxYearData(2002, 0, 0, 0),
      2003 -> CarbonFluxYearData(2003, 0, 0, 0),
      2004 -> CarbonFluxYearData(2004, 0, 0, 0),
      2005 -> CarbonFluxYearData(2005, 0, 0, 0),
      2006 -> CarbonFluxYearData(2006, 0, 0, 0),
      2007 -> CarbonFluxYearData(2007, 0, 0, 0),
      2008 -> CarbonFluxYearData(2008, 0, 0, 0),
      2009 -> CarbonFluxYearData(2009, 0, 0, 0),
      2010 -> CarbonFluxYearData(2010, 0, 0, 0),
      2011 -> CarbonFluxYearData(2011, 0, 0, 0),
      2012 -> CarbonFluxYearData(2012, 0, 0, 0),
      2013 -> CarbonFluxYearData(2013, 0, 0, 0),
      2014 -> CarbonFluxYearData(2014, 0, 0, 0),
      2015 -> CarbonFluxYearData(2015, 0, 0, 0),
      2016 -> CarbonFluxYearData(2016, 0, 0, 0),
      2017 -> CarbonFluxYearData(2017, 0, 0, 0),
      2018 -> CarbonFluxYearData(2018, 0, 0, 0)
    )

  //def mapValuesToList(map: Map[Int, LossYearData]): List[LossYearData] = ???

  def toList(map: Map[Int, CarbonFluxYearData]): List[CarbonFluxYearData] =
    map.values.toList.sorted

}
