package com.tfedorov.datachecker.saver

import com.tfedorov.datachecker.console.ConsolePrinter
import com.tfedorov.datachecker.utils.FileNameUtils.folderNameOpt
import com.tfedorov.datachecker.{AppContext, CalculationResult, ColumnResult}
import com.tfedorov.datachecker.console.ConsolePrinter
import com.tfedorov.datachecker.{AppContext, CalculationResult, ColumnResult}
import org.apache.spark.sql.DataFrame

object ResultsShower {

  def outputResults(results: CalculationResult)(implicit appContext: AppContext): Unit = {
    ConsolePrinter.printArgs(appContext)
    ConsolePrinter.printResults(results)
    results.columns.foreach(saveReport)
  }

  private def saveReport(entity: (String, ColumnResult))(implicit appArgs: AppContext): Unit = {
    val (reportName, ColumnResult(_, _, reportDF)) = entity

    folderNameOpt(reportName)
      .foreach { awsPath =>
        if (reportDF.count() > 10000)
          saveZipped(reportDF)(awsPath)
        else
          save(reportDF)(awsPath)
      }
    /*
    if (appArgs.outputDirPath.isEmpty)
          reportDF.show()
    */
  }

  private def saveZipped(reportDF: DataFrame): String => Unit = {
    reportDF.repartition(1).write.option("header", "true").option("codec", "org.apache.hadoop.io.compress.GzipCodec").csv(_)
  }

  private def save(reportDF: DataFrame): String => Unit = {
    reportDF.repartition(1).write.option("header", "true").csv(_)
  }

}
