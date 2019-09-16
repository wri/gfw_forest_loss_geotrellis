package org.globalforestwatch.summarystats.annualupdate

import scala.collection.mutable.Map

object AnnualUpdateYearDataMap {
  def empty: Map[Int, AnnualUpdateYearData] =
    Map(
      2001 -> AnnualUpdateYearData(2001, 0, 0, 0, 0, 0),
      2002 -> AnnualUpdateYearData(2002, 0, 0, 0, 0, 0),
      2003 -> AnnualUpdateYearData(2003, 0, 0, 0, 0, 0),
      2004 -> AnnualUpdateYearData(2004, 0, 0, 0, 0, 0),
      2005 -> AnnualUpdateYearData(2005, 0, 0, 0, 0, 0),
      2006 -> AnnualUpdateYearData(2006, 0, 0, 0, 0, 0),
      2007 -> AnnualUpdateYearData(2007, 0, 0, 0, 0, 0),
      2008 -> AnnualUpdateYearData(2008, 0, 0, 0, 0, 0),
      2009 -> AnnualUpdateYearData(2009, 0, 0, 0, 0, 0),
      2010 -> AnnualUpdateYearData(2010, 0, 0, 0, 0, 0),
      2011 -> AnnualUpdateYearData(2011, 0, 0, 0, 0, 0),
      2012 -> AnnualUpdateYearData(2012, 0, 0, 0, 0, 0),
      2013 -> AnnualUpdateYearData(2013, 0, 0, 0, 0, 0),
      2014 -> AnnualUpdateYearData(2014, 0, 0, 0, 0, 0),
      2015 -> AnnualUpdateYearData(2015, 0, 0, 0, 0, 0),
      2016 -> AnnualUpdateYearData(2016, 0, 0, 0, 0, 0),
      2017 -> AnnualUpdateYearData(2017, 0, 0, 0, 0, 0),
      2018 -> AnnualUpdateYearData(2018, 0, 0, 0, 0, 0)
    )

  //def mapValuesToList(map: Map[Int, LossYearData]): List[LossYearData] = ???

  def toList(map: Map[Int, AnnualUpdateYearData]): List[AnnualUpdateYearData] =
    map.values.toList.sorted

}
