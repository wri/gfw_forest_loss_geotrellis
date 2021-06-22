package org.globalforestwatch.summarystats

import org.apache.log4j.Logger
import org.globalforestwatch.util.Util.getAnyMapValue

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

trait SummaryAnalysis {

  val name: String
  //  val logger: Logger = Logger.getLogger(name)

  def getOutputUrl(kwargs: Map[String, Any], outputName: String = name): String = {
    val noOutputPathSuffix: Boolean = getAnyMapValue[Boolean](kwargs, "noOutputPathSuffix")
    val outputPath: String = getAnyMapValue[String](kwargs, "outputUrl")

    if (noOutputPathSuffix) outputPath
    else s"${outputPath}/${outputName}_" + DateTimeFormatter
      .ofPattern("yyyyMMdd_HHmm")
      .format(LocalDateTime.now)

  }

}
