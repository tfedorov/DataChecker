package com.tfedorov.datachecker.sparkCustom

import com.tfedorov.datachecker.utils.FileNameUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object SessionExtractor extends Logging {

  def extractSession(): SparkSession = {
    var sessionBuilder = SparkSession.builder
    if (isMacOS) {
      log.error("***** I use local master")
      sessionBuilder = sessionBuilder.master("local")
    }

    sessionBuilder
      .appName(this.getClass.getSimpleName + FileNameUtils.datePrefix)
      .getOrCreate()
  }

  private def isMacOS: Boolean = System.getProperty("os.name").toLowerCase.contains("mac")

}
