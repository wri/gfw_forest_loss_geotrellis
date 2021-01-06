package org.globalforestwatch.summarystats.carbonflux_minimal

import scala.collection.mutable.Map

object CarbonFluxMinimalYearDataMap {
  def empty: Map[Int, CarbonFluxMinimalYearData] =
    Map(
      2001 -> CarbonFluxMinimalYearData(2001, 0, 0, 0, 0, 0),
      2002 -> CarbonFluxMinimalYearData(2002, 0, 0, 0, 0, 0),
      2003 -> CarbonFluxMinimalYearData(2003, 0, 0, 0, 0, 0),
      2004 -> CarbonFluxMinimalYearData(2004, 0, 0, 0, 0, 0),
      2005 -> CarbonFluxMinimalYearData(2005, 0, 0, 0, 0, 0),
      2006 -> CarbonFluxMinimalYearData(2006, 0, 0, 0, 0, 0),
      2007 -> CarbonFluxMinimalYearData(2007, 0, 0, 0, 0, 0),
      2008 -> CarbonFluxMinimalYearData(2008, 0, 0, 0, 0, 0),
      2009 -> CarbonFluxMinimalYearData(2009, 0, 0, 0, 0, 0),
      2010 -> CarbonFluxMinimalYearData(2010, 0, 0, 0, 0, 0),
      2011 -> CarbonFluxMinimalYearData(2011, 0, 0, 0, 0, 0),
      2012 -> CarbonFluxMinimalYearData(2012, 0, 0, 0, 0, 0),
      2013 -> CarbonFluxMinimalYearData(2013, 0, 0, 0, 0, 0),
      2014 -> CarbonFluxMinimalYearData(2014, 0, 0, 0, 0, 0),
      2015 -> CarbonFluxMinimalYearData(2015, 0, 0, 0, 0, 0),
      2016 -> CarbonFluxMinimalYearData(2016, 0, 0, 0, 0, 0),
      2017 -> CarbonFluxMinimalYearData(2017, 0, 0, 0, 0, 0),
      2018 -> CarbonFluxMinimalYearData(2018, 0, 0, 0, 0, 0),
      2019 -> CarbonFluxMinimalYearData(2019, 0, 0, 0, 0, 0)
    )

  //def mapValuesToList(map: Map[Int, LossYearData]): List[LossYearData] = ???

  def toList(map: Map[Int, CarbonFluxMinimalYearData]): List[CarbonFluxMinimalYearData] =
    map.values.toList.sorted

}
