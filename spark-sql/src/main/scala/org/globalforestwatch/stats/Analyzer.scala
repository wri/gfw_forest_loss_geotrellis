package org.globalforestwatch.stats

import cats.Monoid
import geotrellis.raster.{RasterExtent, Tile}
import geotrellis.raster.rasterize._
import geotrellis.vector._
import org.globalforestwatch.raster.RasterLayer

trait Analyzer[SUMMARY] {
  def layers: Seq[RasterLayer]

  // Note: tiles for optional layers may be null; tiles can only be null for optional
  // layers; missing required tiles must be caught prior to calling analyze
  // Note the expectation is that the SUMMARY object can be accumulated in-place as this
  // is an inner loop that will be visited very frequently
  def update(tiles: Seq[Tile], re: RasterExtent, acc: SUMMARY)(col: Int, row: Int)(implicit ev: Monoid[SUMMARY]): Unit

  def analyze(tiles: Seq[Tile], re: RasterExtent, geom: Geometry)(implicit ev: Monoid[SUMMARY]): SUMMARY = {
    val acc = Monoid[SUMMARY].empty

    re.foreach(geom)(update(tiles, re, acc)(_, _))

    acc
  }
}
