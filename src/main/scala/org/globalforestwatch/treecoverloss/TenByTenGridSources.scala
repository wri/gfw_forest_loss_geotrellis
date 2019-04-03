package org.globalforestwatch.treecoverloss

import java.io.FileNotFoundException
import com.typesafe.scalalogging.LazyLogging
import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource
import geotrellis.raster.{Raster, Tile}
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

  def fetchWindow(window: Extent):Tile = source.read(window).get.tile.band(0)

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

  def fetchWindow(window: Extent):Option[Tile] = {
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

  def readWindow(window: Extent): Either[Throwable, Raster[TreeLossTile]] = {

    for {
      // Failure for any of these reads will result in function returning Left[Throwable]
      // These are effectively required fields without which we can't make sense of the analysis
      loss <- Either.catchNonFatal(new requiredTile(MangroveBiomass.source(grid)).fetchWindow(window)).right
      gain <- Either.catchNonFatal(new requiredTile(TreeCoverGain.source(grid)).fetchWindow(window)).right
      tcd2000 <- Either.catchNonFatal(new requiredTile(TreeCoverDensity2000.source(grid)).fetchWindow(window)).right
      tcd2010 <- Either.catchNonFatal(new requiredTile(TreeCoverDensity2010.source(grid)).fetchWindow(window)).right
      co2Pixel <- Either.catchNonFatal(new requiredTile(Carbon.source(grid)).fetchWindow(window)).right
      biomass <- Either.catchNonFatal(new requiredTile(BiomassPerHectar.source(grid)).fetchWindow(window)).right

    } yield {
      // Failure for these will be converted to optional result and propagated with TreeLossTile
      val mangroveBiomass: Option[Tile] = new optionalTile(MangroveBiomass.source(grid)).fetchWindow(window)
      val drivers: Option[Tile] = new optionalTile(TreeCoverLossDrivers.source(grid)).fetchWindow(window)
      val globalLandCover: Option[Tile] = new optionalTile(GlobalLandcover.source(grid)).fetchWindow(window)
      val primaryForest: Option[Tile] = new optionalTile(PrimaryForest.source(grid)).fetchWindow(window)
      val idnPrimaryForest: Option[Tile] = new optionalTile(IndonesiaPrimaryForest.source(grid)).fetchWindow(window)
      val erosion: Option[Tile] = new optionalTile(Erosion.source(grid)).fetchWindow(window)
      val biodiversitySignificance: Option[Tile] = new optionalTile(BiodiversitySignificance.source(grid)).fetchWindow(window)
      val wdpa: Option[Tile] = new optionalTile(ProtectedAreas.source(grid)).fetchWindow(window)
      val plantations: Option[Tile] = new optionalTile(Plantations.source(grid)).fetchWindow(window)
      val riverBasins: Option[Tile] = new optionalTile(RiverBasins.source(grid)).fetchWindow(window)
      val ecozones: Option[Tile] = new optionalTile(Ecozones.source(grid)).fetchWindow(window)
      val urbanWatersheds: Option[Tile] = new optionalTile(UrbanWatersheds.source(grid)).fetchWindow(window)
      val mangroves1996: Option[Tile] = new optionalTile(Mangroves1996.source(grid)).fetchWindow(window)
      val mangroves2016: Option[Tile] = new optionalTile(Mangroves2016.source(grid)).fetchWindow(window)
      val waterStress: Option[Tile] = new optionalTile(WaterStress.source(grid)).fetchWindow(window)
      val intactForestLandscapes: Option[Tile] = new optionalTile(IntactForestLandscapes.source(grid)).fetchWindow(window)
      val endemicBirdAreas: Option[Tile] = new optionalTile(EndemicBirdAreas.source(grid)).fetchWindow(window)
      val tigerLandscapes: Option[Tile] = new optionalTile(TigerLandscapes.source(grid)).fetchWindow(window)
      val landmark: Option[Tile] = new optionalTile(Landmark.source(grid)).fetchWindow(window)
      val landRights: Option[Tile] = new optionalTile(LandRights.source(grid)).fetchWindow(window)
      val keyBiodiversityAreas: Option[Tile] = new optionalTile(KeyBiodiversityAreas.source(grid)).fetchWindow(window)
      val mining: Option[Tile] = new optionalTile(Mining.source(grid)).fetchWindow(window)
      val rspo: Option[Tile] = new optionalTile(RSPO.source(grid)).fetchWindow(window)
      val peatlands: Option[Tile] = new optionalTile(Peatlands.source(grid)).fetchWindow(window)
      val oilPalm: Option[Tile] = new optionalTile(OilPalm.source(grid)).fetchWindow(window)
      val idnForestMoratorium: Option[Tile] = new optionalTile(IndonesiaForestMoratorium.source(grid)).fetchWindow(window)
      val idnLandCover: Option[Tile] = new optionalTile(IndonesiaLandCover.source(grid)).fetchWindow(window)
      val idnForestArea: Option[Tile] = new optionalTile(IndonesiaForestArea.source(grid)).fetchWindow(window)
      val mexProtectedAreas: Option[Tile] = new optionalTile(MexicoProtectedAreas.source(grid)).fetchWindow(window)
      val mexPaymentForEcosystemServices: Option[Tile] = new optionalTile(MexicoPaymentForEcosystemServices.source(grid)).fetchWindow(window)
      val mexForestZoning: Option[Tile] = new optionalTile(MexicoForestZoning.source(grid)).fetchWindow(window)
      val perProductionForest: Option[Tile] = new optionalTile(PeruProductionForest.source(grid)).fetchWindow(window)
      val perProtectedAreas: Option[Tile] = new optionalTile(PeruProtectedAreas.source(grid)).fetchWindow(window)
      val perForestConcessions: Option[Tile] = new optionalTile(PeruForestConcessions.source(grid)).fetchWindow(window)
      val braBiomes: Option[Tile] = new optionalTile(BrazilBiomes.source(grid)).fetchWindow(window)
      val woodFiber: Option[Tile] = new optionalTile(WoodFiber.source(grid)).fetchWindow(window)
      val resourceRights: Option[Tile] = new optionalTile(ResourceRights.source(grid)).fetchWindow(window)
      val logging: Option[Tile] = new optionalTile(Logging.source(grid)).fetchWindow(window)

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
