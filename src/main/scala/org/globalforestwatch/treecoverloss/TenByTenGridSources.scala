package org.globalforestwatch.treecoverloss

import java.io.FileNotFoundException

import com.typesafe.scalalogging.LazyLogging
import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource
import geotrellis.raster.{MultibandTile, Raster, Tile}
import geotrellis.vector.Extent
import cats.implicits._
import com.amazonaws.services.s3.AmazonS3URI


class tileSource(uri: String) {

  lazy val required: GeoTiffRasterSource = TenByTenGridSources.requiredSource(uri)
  lazy val optional: Option[GeoTiffRasterSource] = TenByTenGridSources.optionalSource(uri)

  def requiredTile(window: Extent):Tile = required.read(window).get.tile.band(0)

  def optionalTile(window: Extent):Option[Tile] = {
    for {
      source <- optional
      raster <- Either.catchNonFatal(source.read(window).get.tile.band(0)).toOption
    } yield raster
  }
}


/**
  * @param grid top left corner, padded from east ex: "10N_010E"
  */
case class TenByTenGridSources(grid: String) extends LazyLogging {
  val lossSourceUri = s"s3://wri-users/tmaschler/prep_tiles/loss/${grid}.tif"

  val gainSourceUri = s"s3://wri-users/tmaschler/prep_tiles/gain/${grid}.tif"

  val tcd2000SourceUri = s"s3://wri-users/tmaschler/prep_tiles/tcd_2000/${grid}.tif"

  val tcd2010SourceUri = s"s3://wri-users/tmaschler/prep_tiles/tcd_2010/${grid}.tif"

  val co2PixelSourceUri = s"s3://wri-users/tmaschler/prep_tiles/co2_pixel/${grid}.tif"

  val biomassSourceUri = s"s3://wri-users/tmaschler/prep_tiles/biomass/${grid}.tif"

  val mangroveBiomassSourceUri = s"s3://wri-users/tmaschler/prep_tiles/mangrove_biomass/${grid}.tif"

  val driversSourceUri = s"s3://wri-users/tmaschler/prep_tiles/drivers/${grid}.tif"

  val globalLandCoverSourceUri = s"s3://wri-users/tmaschler/prep_tiles/global_landcover/${grid}.tif"

  val primaryForestSourceUri = s"s3://wri-users/tmaschler/prep_tiles/primary_forest/${grid}.tif"

  val idnPrimaryForestSourceUri = s"s3://wri-users/tmaschler/prep_tiles/idn_primary_forest/${grid}.tif"

  val erosionSourceUri = s"s3://wri-users/tmaschler/prep_tiles/erosion/${grid}.tif"

  val biodiversitySignificanceSourceUri = s"s3://wri-users/tmaschler/prep_tiles/biodiversity_significance/${grid}.tif"

  val wdpaSourceUri = s"s3://wri-users/tmaschler/prep_tiles/wdpa/${grid}.tif"

  val plantationsSourceUri = s"s3://wri-users/tmaschler/prep_tiles/plantations/${grid}.tif"

  val riverBasinsSourceUri = s"s3://wri-users/tmaschler/prep_tiles/river_basins/${grid}.tif"

  val ecozonesSourceUri = s"s3://wri-users/tmaschler/prep_tiles/ecozones/${grid}.tif"

  val urbanWatershedsSourceUri = s"s3://wri-users/tmaschler/prep_tiles/urb_watersheds/${grid}.tif"

  val mangroves1996SourceUri = s"s3://wri-users/tmaschler/prep_tiles/mangroves_1996/${grid}.tif"

  val mangroves2016SourceUri = s"s3://wri-users/tmaschler/prep_tiles/mangroves_2016/${grid}.tif"

  val waterStressSourceUri = s"s3://wri-users/tmaschler/prep_tiles/water_stress/${grid}.tif"

  val intactForestLandscapesSourceUri = s"s3://wri-users/tmaschler/prep_tiles/ifl/${grid}.tif"

  val endemicBirdAreasSourceUri = s"s3://wri-users/tmaschler/prep_tiles/endemic_bird_areas/${grid}.tif"

  val tigerLandscapesSourceUri = s"s3://wri-users/tmaschler/prep_tiles/tiger_landscapes/${grid}.tif"

  val landmarkSourceUri = s"s3://wri-users/tmaschler/prep_tiles/landmark/${grid}.tif"

  val landRightsSourceUri = s"s3://wri-users/tmaschler/prep_tiles/land_rights/${grid}.tif"

  val keyBiodiversityAreasSourceUri = s"s3://wri-users/tmaschler/prep_tiles/kba/${grid}.tif"

  val miningSourceUri = s"s3://wri-users/tmaschler/prep_tiles/mining/${grid}.tif"

  val rspoSourceUri = s"s3://wri-users/tmaschler/prep_tiles/rspo/${grid}.tif"

  val peatlandsSourceUri = s"s3://wri-users/tmaschler/prep_tiles/peatlands/${grid}.tif"

  val oilPalmSourceUri = s"s3://wri-users/tmaschler/prep_tiles/oil_palm/${grid}.tif"

  val idnForestMoratoriumSourceUri = s"s3://wri-users/tmaschler/prep_tiles/idn_forest_moratorium/${grid}.tif"

  val idnLandCoverSourceUri = s"s3://wri-users/tmaschler/prep_tiles/idn_land_cover/${grid}.tif"

  val mexProtectedAreasSourceUri = s"s3://wri-users/tmaschler/prep_tiles/mex_protected_areas/${grid}.tif"

  val mexPaymentForEcosystemServicesSourceUri = s"s3://wri-users/tmaschler/prep_tiles/mex_psa/${grid}.tif"

  val mexForestZoningSourceUri = s"s3://wri-users/tmaschler/prep_tiles/mex_forest_zoning/${grid}.tif"

  val perProductionForestSourceUri = s"s3://wri-users/tmaschler/prep_tiles/per_permanent_production_forests/${grid}.tif"

  val perProtectedAreasSourceUri = s"s3://wri-users/tmaschler/prep_tiles/per_protected_areas/${grid}.tif"

  val perForestConcessionsSourceUri = s"s3://wri-users/tmaschler/prep_tiles/per_forest_concessions/${grid}.tif"

  val braBiomesSourceUri = s"s3://wri-users/tmaschler/prep_tiles/bra_biomes/${grid}.tif"

  val woodFiberSourceUri = s"s3://wri-users/tmaschler/prep_tiles/wood_fiber/${grid}.tif"

  val resourceRightsSourceUri = s"s3://wri-users/tmaschler/prep_tiles/resource_rights/${grid}.tif"

  val loggingSourceUri = s"s3://wri-users/tmaschler/prep_tiles/logging/${grid}.tif"


  def readWindow(window: Extent): Either[Throwable, Raster[TreeLossTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      loss <- Either.catchNonFatal(new tileSource(mangroveBiomassSourceUri).requiredTile(window)).right
      gain <- Either.catchNonFatal(new tileSource(gainSourceUri).requiredTile(window)).right
      tcd2000 <- Either.catchNonFatal(new tileSource(tcd2000SourceUri).requiredTile(window)).right
      tcd2010 <- Either.catchNonFatal(new tileSource(tcd2010SourceUri).requiredTile(window)).right
      co2Pixel <- Either.catchNonFatal(new tileSource(co2PixelSourceUri).requiredTile(window)).right
      biomass <- Either.catchNonFatal(new tileSource(biomassSourceUri).requiredTile(window)).right

    } yield {
      // Failure for these will be converted to optional result and propagated with TreeLossTile
      val mangroveBiomass: Option[Tile] = new tileSource(mangroveBiomassSourceUri).optionalTile(window)
      val drivers: Option[Tile] = new tileSource(driversSourceUri).optionalTile(window)
      val globalLandCover: Option[Tile] = new tileSource(globalLandCoverSourceUri).optionalTile(window)
      val primaryForest: Option[Tile] = new tileSource(primaryForestSourceUri).optionalTile(window)
      val idnPrimaryForest: Option[Tile] = new tileSource(idnPrimaryForestSourceUri).optionalTile(window)
      val erosion: Option[Tile] = new tileSource(erosionSourceUri).optionalTile(window)
      val biodiversitySignificance: Option[Tile] = new tileSource(biodiversitySignificanceSourceUri).optionalTile(window)
      val wdpa: Option[Tile] = new tileSource(wdpaSourceUri).optionalTile(window)
      val plantations: Option[Tile] = new tileSource(plantationsSourceUri).optionalTile(window)
      val riverBasins: Option[Tile] = new tileSource(riverBasinsSourceUri).optionalTile(window)
      val ecozones: Option[Tile] = new tileSource(ecozonesSourceUri).optionalTile(window)
      val urbanWatersheds: Option[Tile] = new tileSource(urbanWatershedsSourceUri).optionalTile(window)
      val mangroves1996: Option[Tile] = new tileSource(mangroves1996SourceUri).optionalTile(window)
      val mangroves2016: Option[Tile] = new tileSource(mangroves2016SourceUri).optionalTile(window)
      val waterStress: Option[Tile] = new tileSource(waterStressSourceUri).optionalTile(window)
      val intactForestLandscapes: Option[Tile] = new tileSource(intactForestLandscapesSourceUri).optionalTile(window)
      val endemicBirdAreas: Option[Tile] = new tileSource(endemicBirdAreasSourceUri).optionalTile(window)
      val tigerLandscapes: Option[Tile] = new tileSource(tigerLandscapesSourceUri).optionalTile(window)
      val landmark: Option[Tile] = new tileSource(landmarkSourceUri).optionalTile(window)
      val landRights: Option[Tile] = new tileSource(landRightsSourceUri).optionalTile(window)
      val keyBiodiversityAreas: Option[Tile] = new tileSource(keyBiodiversityAreasSourceUri).optionalTile(window)
      val mining: Option[Tile] = new tileSource(miningSourceUri).optionalTile(window)
      val rspo: Option[Tile] = new tileSource(rspoSourceUri).optionalTile(window)
      val peatlands: Option[Tile] = new tileSource(peatlandsSourceUri).optionalTile(window)
      val oilPalm: Option[Tile] = new tileSource(oilPalmSourceUri).optionalTile(window)
      val idnForestMoratorium: Option[Tile] = new tileSource(idnForestMoratoriumSourceUri).optionalTile(window)
      val idnLandCover: Option[Tile] = new tileSource(idnLandCoverSourceUri).optionalTile(window)
      val mexProtectedAreas: Option[Tile] = new tileSource(mexProtectedAreasSourceUri).optionalTile(window)
      val mexPaymentForEcosystemServices: Option[Tile] = new tileSource(mexPaymentForEcosystemServicesSourceUri).optionalTile(window)
      val mexForestZoning: Option[Tile] = new tileSource(mexForestZoningSourceUri).optionalTile(window)
      val perProductionForest: Option[Tile] = new tileSource(perProductionForestSourceUri).optionalTile(window)
      val perProtectedAreas: Option[Tile] = new tileSource(perProtectedAreasSourceUri).optionalTile(window)
      val perForestConcessions: Option[Tile] = new tileSource(perForestConcessionsSourceUri).optionalTile(window)
      val braBiomes: Option[Tile] = new tileSource(braBiomesSourceUri).optionalTile(window)
      val woodFiber: Option[Tile] = new tileSource(woodFiberSourceUri).optionalTile(window)
      val resourceRights: Option[Tile] = new tileSource(resourceRightsSourceUri).optionalTile(window)
      val logging: Option[Tile] = new tileSource(loggingSourceUri).optionalTile(window)

      val tile = TreeLossTile(
        loss,
        gain,
        tcd2000,
        tcd2010,
        co2Pixel,
      )

      Raster(tile, window)
    }
  }
}

object TenByTenGridSources {
  val s3Client = geotrellis.spark.io.s3.S3Client.DEFAULT

  /** Check if URI exists before trying to open it, return None if no file found */
  def optionalSource(uri: String): Option[GeoTiffRasterSource] = {
    // Removes the expected 404 errors from console log
    val s3uri = new AmazonS3URI(uri)
    if (s3Client.doesObjectExist(s3uri.getBucket, s3uri.getKey)) {
      println(s"Opening: $uri")
      Some(GeoTiffRasterSource(uri))
    } else None
  }

  def requiredSource(uri: String): GeoTiffRasterSource = {
    // Removes the expected 404 errors from console log
    val s3uri = new AmazonS3URI(uri)
    if (! s3Client.doesObjectExist(s3uri.getBucket, s3uri.getKey)) {
      throw new FileNotFoundException(uri)
    }

    GeoTiffRasterSource(uri)
  }
}