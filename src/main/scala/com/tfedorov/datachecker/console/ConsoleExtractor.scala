package com.tfedorov.datachecker.console

import com.tfedorov.datachecker.AppContext

object ConsoleExtractor {

  private val CORRECT_EXAMPLE = "--original s3://path_to_origin_file --checked s3://path_to_checked_file --parser AuWatch/WallMart/WallMartCsv --columns callLetter,dayPart"

  def apply(args: Array[String]): AppContext = {

    if (args.length < 8)
      throw new IllegalArgumentException("Number of input parameters must be 8 or 10. e.g :" + CORRECT_EXAMPLE)

    buildAppArgs(args)
  }

  private def buildAppArgs(args: Array[String]): AppContext = {
    var result = AppContext.emptyAppArgs

    args.toList.sliding(2, 2).foreach {
      case "--original" :: sourceValue :: Nil => result = result.copy(originalInputPath = sourceValue)

      case "--checked" :: sourceValue :: Nil => result = result.copy(checkedInputPath = sourceValue)

      case "--parser" :: job :: Nil => result = result.copy(job = job)

      case "--columns" :: columns :: Nil => result = result.copy(columns = columns)

      case "--output" :: output :: Nil => result = result.copy(outputDirPath = Some(output))

      case any => println(s"parameters $any ?")
    }
    result
  }

}
