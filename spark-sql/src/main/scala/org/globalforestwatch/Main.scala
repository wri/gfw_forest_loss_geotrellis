package org.globalforestwatch

import com.monovore.decline._
import org.globalforestwatch.stats.afi._

object Main {
  val name = "gfw-raster-stats"
  val header = "Compute summary statistics for GFW data"
  val main = {
    AFiCommand.command
  }
  val command = Command(name, header, true)(main)

  final def main(args: Array[String]): Unit = {
    command.parse(args, sys.env) match {
      case Left(help) =>
        System.err.println(help)
        System.exit(2)
      case Right(_) => ()
    }
  }
}
