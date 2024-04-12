package org.globalforestwatch.summarystats.treecoverloss

import scala.collection.mutable.Map

object TreeLossYearDataMap {
  def empty: Map[Int, TreeLossYearData] =
    Map(
      2001 -> TreeLossYearData(2001, 0, 0, 0, 0, 0),
      2002 -> TreeLossYearData(2002, 0, 0, 0, 0, 0),
      2003 -> TreeLossYearData(2003, 0, 0, 0, 0, 0),
      2004 -> TreeLossYearData(2004, 0, 0, 0, 0, 0),
      2005 -> TreeLossYearData(2005, 0, 0, 0, 0, 0),
      2006 -> TreeLossYearData(2006, 0, 0, 0, 0, 0),
      2007 -> TreeLossYearData(2007, 0, 0, 0, 0, 0),
      2008 -> TreeLossYearData(2008, 0, 0, 0, 0, 0),
      2009 -> TreeLossYearData(2009, 0, 0, 0, 0, 0),
      2010 -> TreeLossYearData(2010, 0, 0, 0, 0, 0),
      2011 -> TreeLossYearData(2011, 0, 0, 0, 0, 0),
      2012 -> TreeLossYearData(2012, 0, 0, 0, 0, 0),
      2013 -> TreeLossYearData(2013, 0, 0, 0, 0, 0),
      2014 -> TreeLossYearData(2014, 0, 0, 0, 0, 0),
      2015 -> TreeLossYearData(2015, 0, 0, 0, 0, 0),
      2016 -> TreeLossYearData(2016, 0, 0, 0, 0, 0),
      2017 -> TreeLossYearData(2017, 0, 0, 0, 0, 0),
      2018 -> TreeLossYearData(2018, 0, 0, 0, 0, 0),
      2019 -> TreeLossYearData(2019, 0, 0, 0, 0, 0),
      2020 -> TreeLossYearData(2020, 0, 0, 0, 0, 0),
      2021 -> TreeLossYearData(2021, 0, 0, 0, 0, 0),
      2022 -> TreeLossYearData(2022, 0, 0, 0, 0, 0),
      2023 -> TreeLossYearData(2023, 0, 0, 0, 0, 0),
    )

  //def mapValuesToList(map: Map[Int, LossYearData]): List[LossYearData] = ???

  def toList(map: Map[Int, TreeLossYearData]): List[TreeLossYearData] =
    map.values.toList.sorted

}
