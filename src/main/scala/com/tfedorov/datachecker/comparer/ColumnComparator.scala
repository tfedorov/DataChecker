package com.tfedorov.datachecker.comparer

import com.tfedorov.datachecker.comparer.auwatch.AuWatchComparator
import com.tfedorov.datachecker.comparer.smi.SMIComparator
import com.tfedorov.datachecker.comparer.std.STDComparator
import com.tfedorov.datachecker.comparer.wallmart.{Parser, WallMartComparator}
import com.tfedorov.datachecker.{AppContext, CalculationResult}
import org.apache.spark.sql.SparkSession

trait ColumnComparator {

  def resolvedColumns: Seq[String]

  def compareColumns(): CalculationResult
}

object ColumnComparator {

  def resolve(appContext: AppContext)(implicit session: SparkSession): ColumnComparator = {
    appContext.job.toLowerCase.trim match {
      case _Comparators.Auwatch => AuWatchComparator(appContext)

      case _Comparators.WallMart => WallMartComparator(appContext, Parser.originalSubstring)

      case _Comparators.WallMartCSV => WallMartComparator(appContext, Parser.originalCsv)

      case _Comparators.SMI => SMIComparator(appContext)

      case _Comparators.STD => STDComparator(appContext)

      case _Comparators.NLNS => std.STDComparator(appContext)
    }
  }

  private[comparer] object _Comparators {
    private[comparer] val Auwatch = "auwatch"
    private[comparer] val WallMart = "wallmart"
    private[comparer] val WallMartCSV = "wallmartcsv"
    private[comparer] val SMI = "smi"
    private[comparer] val STD = "std"
    private[comparer] val NLNS = "nlns"
  }

}
