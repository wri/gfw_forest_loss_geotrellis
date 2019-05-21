package org.globalforestwatch.treecoverloss_simple_60

import scala.collection.mutable.Map

object TreeLossYearDataMap {
  def empty: Map[Int, TreeLossYearData] =
    Map(
      2001 -> TreeLossYearData(2001, 0),
      2002 -> TreeLossYearData(2002, 0),
      2003 -> TreeLossYearData(2003, 0),
      2004 -> TreeLossYearData(2004, 0),
      2005 -> TreeLossYearData(2005, 0),
      2006 -> TreeLossYearData(2006, 0),
      2007 -> TreeLossYearData(2007, 0),
      2008 -> TreeLossYearData(2008, 0),
      2009 -> TreeLossYearData(2009, 0),
      2010 -> TreeLossYearData(2010, 0),
      2011 -> TreeLossYearData(2011, 0),
      2012 -> TreeLossYearData(2012, 0),
      2013 -> TreeLossYearData(2013, 0),
      2014 -> TreeLossYearData(2014, 0),
      2015 -> TreeLossYearData(2015, 0),
      2016 -> TreeLossYearData(2016, 0),
      2017 -> TreeLossYearData(2017, 0),
      2018 -> TreeLossYearData(2018, 0)
    )

  //def mapValuesToList(map: Map[Int, LossYearData]): List[LossYearData] = ???

  def toList(map: Map[Int, TreeLossYearData]): List[TreeLossYearData] =
    map.values.toList.sorted

}
