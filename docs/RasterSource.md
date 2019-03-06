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

```scala
scala> import geotrellis.contrib.vlm.geotiff._
import geotrellis.contrib.vlm.geotiff._

scala> val rs = GeoTiffRasterSource("s3://azavea-datahub/raw/ned-13arcsec-geotiff/imgn44w073_13.tif")
rs: geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource = GeoTiffRasterSource(s3://azavea-datahub/raw/ned-13arcsec-geotiff/imgn44w073_13.tif,None)

scala> rs.crs
res0: geotrellis.proj4.CRS = EPSG:4326

scala> rs.extent
res1: geotrellis.vector.Extent = Extent(-73.00055555556, 42.99944444444, -71.99944444444448, 44.000555555555515)

scala> rs.dimensions
res2: (Int, Int) = (10812,10812)
```

## Windowed Read Access

GeoTrellis `Raster[MultibandTile]` can be read for any window described by `Extent` or pixel `GridBounds` intersecting the raster. The extent of the resulting raster may not match the query extent but will always be aligned to pixel edges of the result raster. When the query extent does not intersect the underlying raster `None` will be returned from the `read` methods.

If the query extent intersects but expands past the source bounds the result will be snapped back to those bounds.

```scala
scala> import geotrellis.vector.Extent
import geotrellis.vector.Extent

scala> val raster = rs.read(Extent(-72.97754892, 43.85921846, -72.80676639, 43.97153490)).get
raster: geotrellis.raster.Raster[geotrellis.raster.MultibandTile] = Raster(geotrellis.raster.ArrayMultibandTile@3e379e6,Extent(-72.97759259259693, 43.859166666666006, -72.80675925926285, 43.97157407407391))

scala> raster.extent
res3: geotrellis.vector.Extent = Extent(-72.97759259259693, 43.859166666666006, -72.80675925926285, 43.97157407407391)

scala> raster.dimensions
res4: (Int, Int) = (1845,1214)
```

Additional read methods are provided for reading multiple windows at once. This allows the `RasterSource` implementation to perform optimizations for the read sequence. `GeoTiffRasterSource` ensures it reads each GeoTiff segment only once, even if it contributes to multiple windows. This but this optimization is not a guarantee for all implementations of `RasterSource`.

```scala
scala> rs.readExtents(List(Extent(-72.97531271,43.92968549,-72.91916503,43.96661144), Extent(-72.87748423,43.85790751,-72.82133655,43.89483346)))
res5: Iterator[geotrellis.raster.Raster[geotrellis.raster.MultibandTile]] = non-empty iterator
```

## Virtual Views

`RasterSource` interface provides a way to perform a lazy resampling and reprojection where the resulting `RasterSource` instance has the metadata of modified raster but no raster transform is performed until a read request. This feature is similar to GDAL VRT and in case of `GDALRasterSource` is backed by that feature.

After transformation all read queries and all of the rater metadata is reported in target `CRS`.

```scala
scala> import geotrellis.proj4._
import geotrellis.proj4._

scala> val rsWM = rs.reproject(WebMercator)
rsWM: geotrellis.contrib.vlm.RasterSource = GeoTiffReprojectRasterSource(s3://azavea-datahub/raw/ned-13arcsec-geotiff/imgn44w073_13.tif,WebMercator,Options(NearestNeighbor,0.125,None,None,None),AutoHigherResolution,None)

scala> rsWM.crs
res6: geotrellis.proj4.CRS = WebMercator

scala> rsWM.extent
res7: geotrellis.vector.Extent = Extent(-8126384.672071017, 5311890.756776384, -8014939.525327434, 5465528.157226967)
```

Sometimes its desirable to match the exact pixel alignment of another rasters so that resulting tiles may be combined correctly. `RasterSource!reprojectToGrid` provides this functionality.

```scala
scala> import geotrellis.raster.RasterExtent
import geotrellis.raster.RasterExtent

scala> val rasterExtent = RasterExtent(Extent(-8126384.672071017, 5311890.756776384, -8014939.525327434, 5465528.157226967), cols = 10000, rows = 10000)
rasterExtent: geotrellis.raster.RasterExtent = GridExtent(Extent(-8126384.672071017, 5311890.756776384, -8014939.525327434, 5465528.157226967),11.14451467435835,15.363740045058261)

scala> rs.reprojectToGrid(WebMercator, rasterExtent)
res8: geotrellis.contrib.vlm.RasterSource = GeoTiffReprojectRasterSource(s3://azavea-datahub/raw/ned-13arcsec-geotiff/imgn44w073_13.tif,WebMercator,Options(NearestNeighbor,0.125,Some(GridExtent(Extent(-8126384.672071017, 5311890.756776384, -8014939.525327434, 5465528.157226967),11.14451467435835,15.363740045058261)),None,None),AutoHigherResolution,None)
```

## Usage with Spark

`RasterSource` instances are intended to be used with Spark. As such they are `Serializable` and can be safely used inside a Spark job.

In `geotrellis-spark` package it is common to work with rasters that are keyed by a certain tile layout, `RDD[(SpatialKey, MultibandTile)]`. Having a tile grid allows for easy joins and ability to reason about the relative spatial position of each tile. `LayoutTileSource` provides ability to key regions of `RasterSource` to a given `LayoutDefinition`.

For instance `layoutDefinition` defines the grid on which 1x1 degree GeoTiffs of National Elevation Dataset are published.

```scala
scala> import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.spark.tiling.LayoutDefinition

scala> import geotrellis.raster.TileLayout
import geotrellis.raster.TileLayout

scala> val layoutDefinition = {
     |   val worldExtent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)
     |   val tileLayout = TileLayout(
     |     layoutCols = 360, layoutRows = 180,
     |     tileCols = 10812, tileRows = 10812)
     |   LayoutDefinition(worldExtent, tileLayout)
     | }
layoutDefinition: geotrellis.spark.tiling.LayoutDefinition = GridExtent(Extent(-180.0, -90.0, 180.0, 90.0),9.24898261191269E-5,9.24898261191269E-5)
```

We can define sub-grid with 212x212 tiles and generated keyed regions for it:

```scala
scala> val tileGrid = {
     |   val worldExtent = Extent(-180.0000, -90.0000, 180.0000, 90.0000)
     |   val tileLayout = TileLayout(
     |     layoutCols = 360 * 51, layoutRows = 180 * 51,
     |     tileCols = 10812 / 51, tileRows = 10812 / 51)
     |   LayoutDefinition(worldExtent, tileLayout)
     | }
tileGrid: geotrellis.spark.tiling.LayoutDefinition = GridExtent(Extent(-180.0, -90.0, 180.0, 90.0),9.24898261191269E-5,9.24898261191269E-5)

scala> import geotrellis.contrib.vlm.LayoutTileSource
import geotrellis.contrib.vlm.LayoutTileSource

scala> val tileSource = LayoutTileSource(rs, tileGrid)
tileSource: geotrellis.contrib.vlm.LayoutTileSource = geotrellis.contrib.vlm.LayoutTileSource@49422338

scala> tileSource.keys
res9: Set[geotrellis.spark.SpatialKey] = Set(SpatialKey(5470,2347), SpatialKey(5474,2386), SpatialKey(5468,2369), SpatialKey(5508,2351), SpatialKey(5492,2361), SpatialKey(5492,2357), SpatialKey(5456,2395), SpatialKey(5457,2345), SpatialKey(5456,2362), SpatialKey(5463,2378), SpatialKey(5477,2365), SpatialKey(5501,2348), SpatialKey(5467,2354), SpatialKey(5462,2350), SpatialKey(5487,2348), SpatialKey(5506,2395), SpatialKey(5468,2368), SpatialKey(5477,2353), SpatialKey(5507,2360), SpatialKey(5457,2387), SpatialKey(5472,2377), SpatialKey(5459,2357), SpatialKey(5489,2394), SpatialKey(5485,2379), SpatialKey(5472,2388), SpatialKey(5469,2353), SpatialKey(5496,2389), SpatialKey(5495,2382), SpatialKey(5478,2396), SpatialKey(5457,2380), SpatialKey(5508,2367), SpatialKey(5483,2365), SpatialKey(5502,...

scala> val (rasterKey, raster) = tileSource.readAll.next
rasterKey: geotrellis.spark.SpatialKey = SpatialKey(5470,2347)
raster: geotrellis.raster.MultibandTile = geotrellis.raster.ArrayMultibandTile@694bc58f

scala> val (regionKey, region) = tileSource.keyedRasterRegions.next
regionKey: geotrellis.spark.SpatialKey = SpatialKey(5470,2347)
region: geotrellis.contrib.vlm.RasterRegion = RasterRegion(GeoTiffRasterSource(s3://azavea-datahub/raw/ned-13arcsec-geotiff/imgn44w073_13.tif,None),GridBounds(2763,219,2974,430))
```

Where as `readAll` call produced a `Raster[MultibandTile]`, which forced and eager read `keyedRasterRegions` returned a `RasterRegion`. `RasterRegion` is a reference to portion of a `RasterSource` that can be turned into a raster later. As such its possible to filter and join `RasterRegion` instances cheaply as a stand-in for eventual raster in a Spark job.

```scala
scala> region.source
res10: geotrellis.contrib.vlm.RasterSource = GeoTiffRasterSource(s3://azavea-datahub/raw/ned-13arcsec-geotiff/imgn44w073_13.tif,None)

scala> region.raster
res11: Option[geotrellis.raster.Raster[geotrellis.raster.MultibandTile]] = Some(Raster(geotrellis.raster.ArrayMultibandTile@5848e6de,Extent(-72.74472222222553, 43.96064814814793, -72.72509259259581, 43.98027777777765)))
```
