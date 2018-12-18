package usbuildings

import java.io.ByteArrayOutputStream
import java.util.zip.{GZIPOutputStream, ZipEntry, ZipOutputStream}

import com.amazonaws.services.s3.model.CannedAccessControlList.PublicRead
import geotrellis.spark._
import geotrellis.spark.io.s3._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index.zcurve.Z2
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector._
import geotrellis.vectortile.{Value, VectorTile}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

import scala.util.Try


object GenerateVT {

  lazy val logger = Logger.getRootLogger()

  type VTF[G <: Geometry] = Feature[G, Map[String, Value]]
  // type VTContents = (Seq[VTF[Point]], Seq[VTF[MultiPoint]], Seq[VTF[Line]], Seq[VTF[MultiLine]], Seq[VTF[Polygon]], Seq[VTF[MultiPolygon]])

  case class VTContents(points: Array[VTF[Point]] = Array.empty,
    multipoints: Array[VTF[MultiPoint]] = Array.empty,
    lines: Array[VTF[Line]] = Array.empty,
    multilines: Array[VTF[MultiLine]] = Array.empty,
    polygons: Array[VTF[Polygon]] = Array.empty,
    multipolygons: Array[VTF[MultiPolygon]] = Array.empty) {
    def +(other: VTContents) = VTContents(points ++ other.points,
      multipoints ++ other.multipoints,
      lines ++ other.lines,
      multilines ++ other.multilines,
      polygons ++ other.polygons,
      multipolygons ++ other.multipolygons)
  }

  object VTContents {
    def empty() = VTContents(Array.empty, Array.empty, Array.empty, Array.empty, Array.empty, Array.empty)
  }

  def save(vectorTiles: RDD[(SpatialKey, VectorTile)], zoom: Int, bucket: String, prefix: String) = {
    vectorTiles
      .mapValues { tile =>
        val byteStream = new ByteArrayOutputStream()

        try {
          val gzipStream = new GZIPOutputStream(byteStream)
          try {
            gzipStream.write(tile.toBytes)
          } finally {
            gzipStream.close()
          }
        } finally {
          byteStream.close()
        }

        byteStream.toByteArray
      }
      .saveToS3(
        { sk: SpatialKey => s"s3://${bucket}/${prefix}/${zoom}/${sk.col}/${sk.row}.mvt" },
        putObjectModifier = { o =>
          val md = o.getMetadata

          md.setContentEncoding("gzip")

          o
            .withMetadata(md)
            .withCannedAcl(PublicRead)
        })
  }

  def saveHadoop(vectorTiles: RDD[(SpatialKey, VectorTile)], zoom: Int, uri: String) = {
    vectorTiles
      .mapValues(_.toBytes)
      .saveToHadoop({ sk: SpatialKey => s"${uri}/${zoom}/${sk.col}/${sk.row}.mvt" })
  }

  def saveInZips(vectorTiles: RDD[(SpatialKey, VectorTile)], zoom: Int, bucket: String, prefix: String) = {
    val offset = zoom % 8

    val s3PathFromKey: SpatialKey => String =
    { sk =>
      s"s3://${bucket}/${prefix}/${zoom - offset}/${sk.col}/${sk.row}.zip"
    }

    vectorTiles
      .mapValues(_.toBytes)
      .map { case (sk, data) => (SpatialKey(sk._1 / Math.pow(2, offset).intValue, sk._2 / Math.pow(2, offset).intValue), (sk, data)) }
      .groupByKey
      .mapValues { data =>
        val out = new ByteArrayOutputStream
        val zip = new ZipOutputStream(out)

        data
          .toSeq
          .sortBy { case (sk, _) => Z2(sk.col, sk.row).z }
          .foreach { case (sk, entry)  =>
            zip.putNextEntry(new ZipEntry(s"${zoom}/${sk.col}/${sk.row}.mvt"))
            zip.write(entry)
            zip.closeEntry()
          }

        zip.close()

        out.toByteArray
      }
      .saveToS3(s3PathFromKey, putObjectModifier = { o => o.withCannedAcl(PublicRead) })
  }

  def keyToLayout[G <: Geometry](features: RDD[VTF[G]], layout: LayoutDefinition): RDD[(SpatialKey, (SpatialKey, VTF[G]))] = {
    features.flatMap{ feat =>
      val g = feat.geom
      val keys = layout.mapTransform.keysForGeometry(g)
      keys.flatMap{ k =>
        val SpatialKey(x, y) = k
        if (x < 0 || x >= layout.layoutCols || y < 0 || y >= layout.layoutRows) {
          println(s"Geometry $g exceeds layout bounds in $k (${Try(layout.mapTransform(k))})")
          None
        } else {
          Some(k -> (k, feat))
        }
      }
    }
  }

  def upLevel[G <: Geometry](keyedGeoms: RDD[(SpatialKey, (SpatialKey, VTF[G]))]): RDD[(SpatialKey, (SpatialKey, VTF[G]))] = {
    keyedGeoms.map{ case (key, (_, feat)) => {
      val SpatialKey(x, y) = key
      val newKey = SpatialKey(x/2, y/2)
      (newKey, (newKey, feat))
    }}
  }

}
