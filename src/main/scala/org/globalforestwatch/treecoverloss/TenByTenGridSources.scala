package org.globalforestwatch.treecoverloss

import java.io.FileNotFoundException

import com.typesafe.scalalogging.LazyLogging
import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource
import geotrellis.raster.{MultibandTile, Raster, Tile}
import geotrellis.vector.Extent
import cats.implicits._
import com.amazonaws.services.s3.AmazonS3URI



class requiredTile(uri: String) {

  val s3Client = geotrellis.spark.io.s3.S3Client.DEFAULT

  lazy val source: GeoTiffRasterSource = fetchSource

  def fetchSource: GeoTiffRasterSource = {
    // Removes the expected 404 errors from console log
    val s3uri = new AmazonS3URI(uri)
    if (! s3Client.doesObjectExist(s3uri.getBucket, s3uri.getKey)) {
      throw new FileNotFoundException(uri)
    }
    GeoTiffRasterSource(uri)
  }

  def fetchTile(window: Extent):Tile = source.read(window).get.tile.band(0)

}


class optionalTile(uri: String) {

  val s3Client = geotrellis.spark.io.s3.S3Client.DEFAULT

  lazy val source: Option[GeoTiffRasterSource] = fetchSource

  /** Check if URI exists before trying to open it, return None if no file found */
  def fetchSource: Option[GeoTiffRasterSource] = {
    // Removes the expected 404 errors from console log
    val s3uri = new AmazonS3URI(uri)
    if (s3Client.doesObjectExist(s3uri.getBucket, s3uri.getKey)) {
      println(s"Opening: $uri")
      Some(GeoTiffRasterSource(uri))
    } else None
  }

  def fetchTile(window: Extent):Option[Tile] = {
    for {
      source <- source
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
      loss <- Either.catchNonFatal(new requiredTile(mangroveBiomassSourceUri).fetchTile(window)).right
      gain <- Either.catchNonFatal(new requiredTile(gainSourceUri).fetchTile(window)).right
      tcd2000 <- Either.catchNonFatal(new requiredTile(tcd2000SourceUri).fetchTile(window)).right
      tcd2010 <- Either.catchNonFatal(new requiredTile(tcd2010SourceUri).fetchTile(window)).right
      co2Pixel <- Either.catchNonFatal(new requiredTile(co2PixelSourceUri).fetchTile(window)).right
      biomass <- Either.catchNonFatal(new requiredTile(biomassSourceUri).fetchTile(window)).right

    } yield {
      // Failure for these will be converted to optional result and propagated with TreeLossTile
      val mangroveBiomass: Option[Tile] = new optionalTile(mangroveBiomassSourceUri).fetchTile(window)
      val drivers: Option[Tile] = new optionalTile(driversSourceUri).fetchTile(window)
      val globalLandCover: Option[Tile] = new optionalTile(globalLandCoverSourceUri).fetchTile(window)
      val primaryForest: Option[Tile] = new optionalTile(primaryForestSourceUri).fetchTile(window)
      val idnPrimaryForest: Option[Tile] = new optionalTile(idnPrimaryForestSourceUri).fetchTile(window)
      val erosion: Option[Tile] = new optionalTile(erosionSourceUri).fetchTile(window)
      val biodiversitySignificance: Option[Tile] = new optionalTile(biodiversitySignificanceSourceUri).fetchTile(window)
      val wdpa: Option[Tile] = new optionalTile(wdpaSourceUri).fetchTile(window)
      val plantations: Option[Tile] = new optionalTile(plantationsSourceUri).fetchTile(window)
      val riverBasins: Option[Tile] = new optionalTile(riverBasinsSourceUri).fetchTile(window)
      val ecozones: Option[Tile] = new optionalTile(ecozonesSourceUri).fetchTile(window)
      val urbanWatersheds: Option[Tile] = new optionalTile(urbanWatershedsSourceUri).fetchTile(window)
      val mangroves1996: Option[Tile] = new optionalTile(mangroves1996SourceUri).fetchTile(window)
      val mangroves2016: Option[Tile] = new optionalTile(mangroves2016SourceUri).fetchTile(window)
      val waterStress: Option[Tile] = new optionalTile(waterStressSourceUri).fetchTile(window)
      val intactForestLandscapes: Option[Tile] = new optionalTile(intactForestLandscapesSourceUri).fetchTile(window)
      val endemicBirdAreas: Option[Tile] = new optionalTile(endemicBirdAreasSourceUri).fetchTile(window)
      val tigerLandscapes: Option[Tile] = new optionalTile(tigerLandscapesSourceUri).fetchTile(window)
      val landmark: Option[Tile] = new optionalTile(landmarkSourceUri).fetchTile(window)
      val landRights: Option[Tile] = new optionalTile(landRightsSourceUri).fetchTile(window)
      val keyBiodiversityAreas: Option[Tile] = new optionalTile(keyBiodiversityAreasSourceUri).fetchTile(window)
      val mining: Option[Tile] = new optionalTile(miningSourceUri).fetchTile(window)
      val rspo: Option[Tile] = new optionalTile(rspoSourceUri).fetchTile(window)
      val peatlands: Option[Tile] = new optionalTile(peatlandsSourceUri).fetchTile(window)
      val oilPalm: Option[Tile] = new optionalTile(oilPalmSourceUri).fetchTile(window)
      val idnForestMoratorium: Option[Tile] = new optionalTile(idnForestMoratoriumSourceUri).fetchTile(window)
      val idnLandCover: Option[Tile] = new optionalTile(idnLandCoverSourceUri).fetchTile(window)
      val mexProtectedAreas: Option[Tile] = new optionalTile(mexProtectedAreasSourceUri).fetchTile(window)
      val mexPaymentForEcosystemServices: Option[Tile] = new optionalTile(mexPaymentForEcosystemServicesSourceUri).fetchTile(window)
      val mexForestZoning: Option[Tile] = new optionalTile(mexForestZoningSourceUri).fetchTile(window)
      val perProductionForest: Option[Tile] = new optionalTile(perProductionForestSourceUri).fetchTile(window)
      val perProtectedAreas: Option[Tile] = new optionalTile(perProtectedAreasSourceUri).fetchTile(window)
      val perForestConcessions: Option[Tile] = new optionalTile(perForestConcessionsSourceUri).fetchTile(window)
      val braBiomes: Option[Tile] = new optionalTile(braBiomesSourceUri).fetchTile(window)
      val woodFiber: Option[Tile] = new optionalTile(woodFiberSourceUri).fetchTile(window)
      val resourceRights: Option[Tile] = new optionalTile(resourceRightsSourceUri).fetchTile(window)
      val logging: Option[Tile] = new optionalTile(loggingSourceUri).fetchTile(window)

      val tile = TreeLossTile(
        loss,
        gain,
        tcd2000,
        tcd2010,
        co2Pixel,
        biomass,
        mangroveBiomass,
        drivers,
        globalLandCover,
        primaryForest,
        idnPrimaryForest,
        erosion,
        biodiversitySignificance,
        wdpa,
        plantations,
        riverBasins,
        ecozones,
        urbanWatersheds,
        mangroves1996,
        mangroves2016,
        waterStress,
        intactForestLandscapes,
        endemicBirdAreas,
        tigerLandscapes,
        landmark,
        landRights,
        keyBiodiversityAreas,
        mining,
        rspo,
        peatlands,
        oilPalm,
        idnForestMoratorium,
        idnLandCover,
        mexProtectedAreas,
        mexPaymentForEcosystemServices,
        mexForestZoning,
        perProductionForest,
        perProtectedAreas,
        perForestConcessions,
        braBiomes,
        woodFiber,
        resourceRights,
        logging
      )

      Raster(tile, window)
    }
  }
}
