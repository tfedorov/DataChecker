package com.tfedorov.datachecker

import com.tfedorov.datachecker.comparer.ColumnComparator
import com.tfedorov.datachecker.console.{ConsoleExtractor, ConsolePrinter}
import com.tfedorov.datachecker.saver.ResultsShower
import com.tfedorov.datachecker.sparkCustom.SessionExtractor
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object DataComparatorApp extends App with Logging {

  log.warn("***** START DataComparatorApp *****")
  try {

    implicit val appContext: AppContext = ConsoleExtractor(args)

    ConsolePrinter.printArgs(appContext)

    implicit val session: SparkSession = SessionExtractor.extractSession()

    val comparator = ColumnComparator.resolve(appContext)
    log.warn(s"Selected the ${comparator.getClass.getSimpleName} comparator")

    val resolvedColumns: Seq[String] = comparator.resolvedColumns
    log.warn(s"Compare $resolvedColumns columns")
    appContext.resolvedColumns = resolvedColumns

    val results: CalculationResult = comparator.compareColumns()

    ResultsShower.outputResults(results)
  }
  catch {
    case ex: Throwable => log.error(ex.getMessage)
  }

  log.warn("***** END DataComparatorApp *****")


}
