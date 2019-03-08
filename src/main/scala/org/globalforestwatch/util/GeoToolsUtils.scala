package org.globalforestwatch.util

import org.geotools.data.ogr.OGRDataStore
import org.geotools.data.ogr.bridj.BridjOGRDataStoreFactory

object GeoToolsUtils {
  /** Open GeoTools OGRDataStore */
  def getOgrDataStore(uri: String, driver: Option[String] =  None): OGRDataStore = {
    println(s"Opening: $uri")
    val factory = new BridjOGRDataStoreFactory()
    val params = new java.util.HashMap[String, String]
    params.put("DatasourceName", uri)
    driver.foreach(params.put("DriverName", _))
    factory.createDataStore(params).asInstanceOf[OGRDataStore]
  }

}
