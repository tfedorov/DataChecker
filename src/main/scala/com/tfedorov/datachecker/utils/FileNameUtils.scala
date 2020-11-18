package com.tfedorov.datachecker.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import com.tfedorov.datachecker.AppContext

import scala.reflect.io.File

object FileNameUtils {

  private val format = new SimpleDateFormat("MMMdd-HH_mm")

  def datePrefix: String = format.format(Calendar.getInstance().getTime)

  private def dirNameSuffix(suffix: String, resultFolder: String, job: String): String = {
    val fileName = s"$suffix-diff_${job}_" ++ FileNameUtils.datePrefix
    if (resultFolder.endsWith(File.separator))
      resultFolder + fileName
    else
      resultFolder + File.separator + fileName
  }


  def folderNameOpt(suffix: String)(implicit appArgs: AppContext): Option[String] = {
    appArgs.outputDirPath.map(dirNameSuffix(suffix, _, appArgs.job))
  }
}
