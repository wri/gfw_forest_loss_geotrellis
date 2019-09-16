package org.globalforestwatch.summarystats.annualupdate_minimal

import scala.collection.mutable.Map

object AnnualUpdateMinimalYearDataMap {
  def empty: Map[Int, AnnualUpdateMinimalYearData] =
    Map(
      2001 -> AnnualUpdateMinimalYearData(2001, 0, 0, 0),
      2002 -> AnnualUpdateMinimalYearData(2002, 0, 0, 0),
      2003 -> AnnualUpdateMinimalYearData(2003, 0, 0, 0),
      2004 -> AnnualUpdateMinimalYearData(2004, 0, 0, 0),
      2005 -> AnnualUpdateMinimalYearData(2005, 0, 0, 0),
      2006 -> AnnualUpdateMinimalYearData(2006, 0, 0, 0),
      2007 -> AnnualUpdateMinimalYearData(2007, 0, 0, 0),
      2008 -> AnnualUpdateMinimalYearData(2008, 0, 0, 0),
      2009 -> AnnualUpdateMinimalYearData(2009, 0, 0, 0),
      2010 -> AnnualUpdateMinimalYearData(2010, 0, 0, 0),
      2011 -> AnnualUpdateMinimalYearData(2011, 0, 0, 0),
      2012 -> AnnualUpdateMinimalYearData(2012, 0, 0, 0),
      2013 -> AnnualUpdateMinimalYearData(2013, 0, 0, 0),
      2014 -> AnnualUpdateMinimalYearData(2014, 0, 0, 0),
      2015 -> AnnualUpdateMinimalYearData(2015, 0, 0, 0),
      2016 -> AnnualUpdateMinimalYearData(2016, 0, 0, 0),
      2017 -> AnnualUpdateMinimalYearData(2017, 0, 0, 0),
      2018 -> AnnualUpdateMinimalYearData(2018, 0, 0, 0)
    )

  //def mapValuesToList(map: Map[Int, LossYearData]): List[LossYearData] = ???

  def toList(map: Map[Int, AnnualUpdateMinimalYearData]): List[AnnualUpdateMinimalYearData] =
    map.values.toList.sorted

}
