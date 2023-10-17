package org.globalforestwatch.util

import breeze.numerics._
import geotrellis.raster.CellSize

object Geodesy {
  def pixelArea(lat: Double, cellSize: CellSize): Double = {

    /** Calculate geodesic area for a pixel using giving pixel size and latitude
      * Assumes WGS 1984 as spatial reference
      */
    val a: Double = 6378137.0 // Semi major axis of WGS 1984 ellipsoid
    val b: Double = 6356752.314245179 // Semi minor axis of WGS 1984 ellipsoid

    val pi: Double = Math.PI

    val dLng: Double = cellSize.width
    val dLat: Double = cellSize.height

    val q: Double = dLng / 360
    val e: Double = sqrt(1 - pow((b / a), 2))

    val area: Double = Math.abs(
      (pi * pow(b, 2) * (2 * atanh(e * sin(toRadians(lat + dLat))) /
        (2 * e) +
        sin(toRadians(lat + dLat)) /
          ((1 + e * sin(toRadians(lat + dLat))) * (1 - e * sin(
            toRadians(lat + dLat)
          ))))) -
        (pi * pow(b, 2) * (2 * atanh(e * sin(toRadians(lat))) / (2 * e) +
          sin(toRadians(lat)) / ((1 + e * sin(toRadians(lat))) * (1 - e * sin(
            toRadians(lat)
          )))))
    ) * q

    area
  }

}
