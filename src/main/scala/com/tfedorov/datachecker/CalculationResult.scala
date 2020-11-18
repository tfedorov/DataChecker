package com.tfedorov.datachecker

import org.apache.spark.sql.DataFrame

import scala.collection.mutable._

case class CalculationResult(originalNumber: Long,
                             checkedNumber: Long,
                             columns: ListBuffer[(String, ColumnResult)] = ListBuffer.empty) {
  def append(columnName: String, columnResult: ColumnResult): Unit = {
    val tuple = (columnName, columnResult)
    columns += tuple
  }
}

case class ColumnResult(matchedNumber: Long, unmatchedNumber: Long, reportDF: DataFrame)

object CalculationResult {
  def empty: CalculationResult = CalculationResult(-1, -1)
}

