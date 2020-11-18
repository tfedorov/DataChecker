package com.tfedorov.datachecker.console

import com.tfedorov.datachecker.{AppContext, CalculationResult, ColumnResult}
import org.apache.spark.internal.Logging

object ConsolePrinter extends Logging {

  def printResults(result: CalculationResult): Unit = {
    val CalculationResult(original, checked, cols) = result
    log.info("+----------+----------+")
    log.info("| Original | Checked  |")
    log.info("+----------+----------+")
    log.info(s"|${long2Str(original)}|${long2Str(checked)}|")
    log.info("+-----------+---------+")
    log.info(",Column,Matched,Unmatched")

    cols.foreach(printColumn)

  }

  def printColumn(columnKeyVal: (String, ColumnResult)): Unit = columnKeyVal match {
    case (column: String, ColumnResult(matched, unmatched, _)) =>
      log.info(s",$column,$matched,$unmatched")
    //log.info(s"|${str2Str(column)}|${long2Str(matched)}|${long2Str(unmatched)}|")
    //log.info("+--------------+----------+---------+")
  }

  private def long2Str(l: Long): String = "%10d".format(l)

  private def str2Str(s: String): String = {
    val str = "%9s".format(s)
    if (str.length > 14)
      str.substring(0, 11) + "..."
    else
      str
  }


  def printArgs(args: AppContext): Unit = {
    args match {
      case AppContext(originalInputPath, checkedInputPath, job, columns, Some(outputPath)) =>

        log.info("  Console parameters ")
        printEdgeTable()
        log.info("|  Number |  Name   | Value |")
        printEdgeTable()
        printColumn(1, "original", originalInputPath)
        printColumn(2, "checked", checkedInputPath)
        printColumn(3, "job", job)
        printColumn(4, "columns", columns)
        printColumn(5, "output", outputPath)
        printEdgeTable()

      case AppContext(originalInputPath, checkedInputPath, job, columns, None) =>

        log.info("  Console parameters ")
        printEdgeTable()
        log.info("|  Number |  Name   | Value |")
        printEdgeTable()
        printColumn(1, "original", originalInputPath)
        printColumn(2, "checked", checkedInputPath)
        printColumn(3, "job", job)
        printColumn(4, "columns", columns)
        printColumn(5, "output", "absent")
        printEdgeTable()
    }
    if (!args.resolvedColumns.isEmpty) {
      ConsolePrinter.printColumn(-4, "resolved", args.resolvedColumns.mkString(","))
      ConsolePrinter.printEdgeTable()
    }
  }

  def printEdgeTable(): Unit = log.info("+---------+---------+-------+")

  def printColumn(number: Int, name: String, value: String): Unit = {
    val numberPar = str2Str(number.toString)
    val namePar = str2Str(name)
    log.info(s"|$numberPar|$namePar|'$value'")
  }

}
