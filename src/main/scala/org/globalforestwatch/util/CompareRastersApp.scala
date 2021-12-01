package org.globalforestwatch

import cats.implicits._
import com.monovore.decline._
import java.io.File

import geotrellis.raster._
import geotrellis.layer._

/** Utility CLI to compare to sets of rasters that share their name.
 * This was once useful to verify that GLAD rasters used by this app are the same that are used in production.
 * Turns out they are so nothing needed to be done, but next step might have been to produce GeoJSON of tiles where mismatch happens.
 */
object CompareRastersApp
    extends CommandApp(
      name = "compare-rasters",
      header = "Compare rasters with same name by pixels",
      main = {
        (
          Opts.option[String]("left", help = "The path that points to data that will be read"),
          Opts.option[String]("right", help = "The path that points to data that will be read")
        ).mapN { (left, right) =>
          import CompareRasters._
          val matched: List[(File, File)] = {
            def makeMap(files: Array[File]): Map[String, List[File]] =
              files.map({ f => f.getName -> List(f) }).toMap

            val leftFiles = listTiffs(left)
            val rightFiles = listTiffs(right)

            val pairs = makeMap(leftFiles) |+| makeMap(rightFiles)

            pairs.values
              .filter{ files =>
                val matched = files.length == 2
                if (! matched) println(s"Unmatched: ${files.head}")
                matched
              }
              .map { case List(l, r) => (l, r) }
              .toList
          }

          matched.par.foreach { case (leftFile, rightFile) =>
            println(s"Comparing: $leftFile with $rightFile")
            val leftRaster = RasterSource(leftFile.toString)
            val rightRaster = RasterSource(rightFile.toString)
            compareRasters(leftRaster, rightRaster)
          }
        }
      }
    )

object CompareRasters {
  def listTiffs(path: String): Array[File] = {
    new File(path).listFiles
      .filter(_.isFile)
      .filter(_.getName().endsWith(".tif"))
  }

  def compareRasters(left: RasterSource, right: RasterSource) = {
    require(left.extent == right.extent)
    require(left.dimensions == right.dimensions)
    val layout = LayoutDefinition(left.gridExtent, tileSize=4000)
    val leftTiles = LayoutTileSource.spatial(left, layout)
    val rightTiles = LayoutTileSource.spatial(right, layout)
    leftTiles.keys.foreach { key =>
      val a = leftTiles.read(key).get
      val b = rightTiles.read(key).get

      require(a == b, s"Not equal at $key!")
    }
  }
}
