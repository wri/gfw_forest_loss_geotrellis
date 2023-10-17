package org.globalforestwatch.util

/**
  * Ported from https://github.com/mapbox/mercantile/
  */
object Mercantile {

  /**
    * Raised when math errors occur beyond ~85 degrees N or S
    *
    * @param message: String
    * @param cause: Throwable
    */
  final case class InvalidLatitudeError(private val message: String = "",
                                        private val cause: Throwable =
                                          None.orNull)
      extends Exception(message, cause)

  /**
    * Base exception
    *
    * @param message: String
    * @param cause: Throwable
    */
  final case class MercantileError(private val message: String = "",
                                   private val cause: Throwable = None.orNull)
      extends Exception(message, cause)

  /**
    * An XYZ web mercator tile
    * Attributes
    * ----------
    * x and y indexes of the tile and zoom level z.
    *
    * @param x: Int
    * @param y: Int
    * @param z: Int
    */
  case class Tile(x: Int, y: Int, z: Int)

  /**
    *
    * @param lng: Double
    * @param lat: Double
    * @return
    */
  def truncateLngLat(lng: Double, lat: Double): (Double, Double) = {

    val newLng = {
      if (lng > 180.0) 180.0
      else if (lng < -180.0) -180.0
      else lng
    }

    val newLat = {
      if (lat > 90.0) 90.0
      else if (lat < -90.0) -90.0
      else lat
    }

    (newLng, newLat)
  }

  /**
    * Get the tile containing a longitude and latitude
    * Parameters
    * ----------
    * lng, lat : float
    * A longitude and latitude pair in decimal degrees.
    * zoom : int
    * The web mercator zoom level.
    * truncate : bool, optional
    * Whether or not to truncate inputs to limits of web mercator.
    * Returns
    * -------
    * Tile
    *
    * @param lng: Double
    * @param lat: Double
    * @param zoom: It
    * @param truncate: Boolean
    * @return
    */
  def tile(lng: Double,
           lat: Double,
           zoom: Int,
           truncate: Boolean = false): Tile = {

    val (newLng, newLat) = {
      if (truncate) truncateLngLat(lng, lat)
      else (lng, lat)
    }

    val latRad = Math.toRadians(newLat)
    val n = Math.pow(2.0, zoom)
    val xTile = math.floor((newLng + 180.0) / 360.0 * n).toInt

    val yTile = {
      try {

        math
          .floor(
            (1.0 - math
              .log(math.tan(latRad) + (1.0 / math.cos(latRad))) / math.Pi) / 2.0 * n
          )
          .toInt
      } catch {
        case _: Throwable => // TODO: narrow down exception
          throw InvalidLatitudeError(
            s"Y can not be computed for latitude $lat radians"
          )
      }
    }
    Tile(xTile, yTile, zoom)
  }

  /**
    * Get the parent of a tile
    * The parent is the tile of one zoom level lower that contains the
    * given "child" tile.
    * Parameters
    * ----------
    * tile : Tile
    * Returns
    * -------
    * Tile
    *
    * @param tile: Tile
    * @return
    */
  def parent(tile: Tile): Tile = {

    // Algorithm ported directly from https://github.com/mapbox/tilebelt.
    if ((tile.x % 2 == 0) && (tile.y % 2 == 0))
      Tile(
        math.floor(tile.x / 2.0).toInt,
        math.floor(tile.y / 2).toInt,
        tile.z - 1
      )
    else if (tile.x % 2 == 0)
      Tile(
        math.floor(tile.x / 2).toInt,
        math.floor((tile.y - 1) / 2).toInt,
        tile.z - 1
      )
    else if (!(tile.x % 2 == 0) && (tile.y % 2 == 0))
      Tile(
        math.floor((tile.x - 1) / 2).toInt,
        math.floor(tile.y / 2).toInt,
        tile.z - 1
      )
    else
      Tile(
        math.floor((tile.x - 1) / 2).toInt,
        math.floor((tile.y - 1) / 2).toInt,
        tile.z - 1
      )
  }

  /**
    * Get the parent of a tile
    * The parent is the tile of one zoom level lower that contains the
    * given "child" tile.
    * Parameters
    * ----------
    * x : Int
    * y : Int
    * z : Int
    *
    * Returns
    * -------
    * Tile
    *
    * @param x: Int
    * @param y: Int
    * @param z: Int
    * @return
    */
  def parent(x: Int, y: Int, z: Int): Tile = {
    parent(Tile(x, y, z))
  }
}
