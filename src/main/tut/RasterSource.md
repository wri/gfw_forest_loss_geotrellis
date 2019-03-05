# RasterSource

`RasterSource` interface is fundamental raster input interface in GeoTrellis.
It abstracts over, file format, storage method and read implementation.

This code is currently being developed under [geotrellis-contrib](https://github.com/geotrellis/geotrellis-contrib) project with plans of merging it with GeoTrellis 3.0 release.

Current implementations exist for:

- GeoTrellis GeoTiff Reader
- GeoTrellis Indexed Layer
- GDAL

## Metadata Access

Metadata will be read separately from pixels and user should expect it to be cached for repeated accessed through available fields.
Additionally `RasterSource` should always be able to describe itself using a URI string.

```tut
import geotrellis.contrib.vlm.geotiff._

val rs = GeoTiffRasterSource("s3://azavea-datahub/raw/ned-13arcsec-geotiff/imgn44w073_13.tif")
rs.crs
rs.extent
rs.dimensions
```

## Windowed Read Access

GeoTrellis `Raster[MultibandTile]` can be read for any window described by `Extent` or pixel `GridBounds` intersecting the raster. The extent of the resulting raster may not match the query extent but will always be aligned to pixel edges of the result raster. When the query extent does not intersect the underlying raster `None` will be returned from the `read` methods.

If the query extent intersects but expands past the source bounds the result will be snapped back to those bounds.

```tut
import geotrellis.vector.Extent
val raster = rs.read(Extent(-72.97754892, 43.85921846, -72.80676639, 43.97153490)).get
raster.extent
raster.dimensions
```

Additional read methods are provided for reading multiple windows at once. This allows the `RasterSource` implementation to perform optimizations for the read sequence. `GeoTiffRasterSource` ensures it reads each GeoTiff segment only once, even if it contributes to multiple windows. This but this optimization is not a guarantee for all implementations of `RasterSource`.

```tut
rs.readExtents(List(Extent(-72.97531271,43.92968549,-72.91916503,43.96661144), Extent(-72.87748423,43.85790751,-72.82133655,43.89483346)))
```

## Virtual Views

`RasterSource` interface provides a way to perform a lazy resampling and reprojection where the resulting `RasterSource` instance has the metadata of modified raster but no raster transform is performed until a read request. This feature is similar to GDAL VRT and in case of `GDALRasterSource` is backed by that feature.

After transformation all read queries and all of the rater metadata is reported in target `CRS`.

```tut
import geotrellis.proj4._

val rsWM = rs.reproject(WebMercator)
rsWM.crs
rsWM.extent
```

Sometimes its desirable to match the exact pixel alignment of another rasters so that resulting tiles may be combined correctly. `RasterSource!reprojectToGrid` provides this functionality.

```tut
import geotrellis.raster.RasterExtent
val rasterExtent = RasterExtent(Extent(-8126384.672071017, 5311890.756776384, -8014939.525327434, 5465528.157226967), cols = 10000, rows = 10000)
rs.reprojectToGrid(WebMercator, rasterExtent)
```

## Usage with Spark

`RasterSource` instances are intended to be used with Spark. As such they are `Serializable` and can be safely used inside a Spark job.

In `geotrellis-spark` package it is common to work with rasters that are keyed by a certain tile layout, `RDD[(SpatialKey, MultibandTile)]`. Having a tile grid allows for easy joins and ability to reason about the relative spatial position of each tile. `LayoutTileSource` provides ability to key regions of `RasterSource` to a given `LayoutDefinition`.

For instance `layoutDefinition` defines the grid on which 1x1 degree GeoTiffs of National Elevation Dataset are published.

```tut
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.raster.TileLayout

val layoutDefinition = {
  val worldExtent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)
  val tileLayout = TileLayout(
    layoutCols = 360, layoutRows = 180,
    tileCols = 10812, tileRows = 10812)
  LayoutDefinition(worldExtent, tileLayout)
}
```

We can define sub-grid with 212x212 tiles and generated keyed regions for it:

```tut
val tileGrid = {
  val worldExtent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)
  val tileLayout = TileLayout(
    layoutCols = 360 * 51, layoutRows = 180 * 51,
    tileCols = 10812 / 51, tileRows = 10812 / 51)
  LayoutDefinition(worldExtent, tileLayout)
}

import geotrellis.contrib.vlm.LayoutTileSource
val tileSource = LayoutTileSource(rs, tileGrid)
tileSource.keys
val (rasterKey, raster) = tileSource.readAll.next
val (regionKey, region) = tileSource.keyedRasterRegions.next
```

Where as `readAll` call produced a `Raster[MultibandTile]`, which forced and eager read `keyedRasterRegions` returned a `RasterRegion`. `RasterRegion` is a reference to portion of a `RasterSource` that can be turned into a raster later. As such its possible to filter and join `RasterRegion` instances cheaply as a stand-in for eventual raster in a Spark job.

```tut
region.source
region.raster
```