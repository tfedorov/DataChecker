package com.tfedorov.datachecker.comparer.wallmart

import com.tfedorov.datachecker.comparer.{ColumnComparator, GroupProcessor, RowInfo}
import com.tfedorov.datachecker.{AppContext, CalculationResult, ColumnResult}
import org.apache.spark.internal.Logging
import org.apache.spark.sql._

protected[comparer] case class WallMartComparator(appArgs: AppContext, originalParser: String => WallMartRow)
                                                 (implicit val session: SparkSession) extends ColumnComparator with Logging {

  import session.sqlContext.implicits._

  override val resolvedColumns: Seq[String] = appArgs.columns.split(",").toList match {
    case "_all" :: Nil => WallMartRow.checkedCols
    case "_BaseLine" :: Nil => Nil
    case "_grp" :: Nil => WallMartRow._grps
    case consoleColumns => consoleColumns
  }
  private var calculationResults: CalculationResult = CalculationResult.empty

  override def compareColumns(): CalculationResult = {
    baseLineMatchedDS.cache()
    resolvedColumns.foreach(compareColumn)
    calculationResults
  }

  lazy val unionDS: Dataset[WallMartRow] = {
    val inputRDD = session.sparkContext.textFile(appArgs.originalInputPath)
    val originalDS: Dataset[WallMartRow] = inputRDD.map[WallMartRow](originalParser).toDS().filter(!_.isQuartOfHour)

    val inputCheckedRDD = session.sparkContext.textFile(appArgs.checkedInputPath)
    val checkedDS: Dataset[WallMartRow] = inputCheckedRDD.map(Parser.checkedParseTry).filter(_.isSuccess).map(_.get).toDS().filter(!_.isQuartOfHour)

    calculationResults = CalculationResult(checkedNumber = checkedDS.count(), originalNumber = originalDS.count())
    originalDS.union(checkedDS)
  }

  private lazy val baseLineMatchedDS: Dataset[Row] = {
    val grouperBaseLine = GroupProcessor(unionDS, WallMartRow.baseLineCols, WallMartRow.checkedCols ++ RowInfo.sourceTextCols)

    val blMatchedDS = grouperBaseLine.matchedRawDS
    val (matched, unMatched) = grouperBaseLine.matchUnmatchNums

    val baseLineRes = ColumnResult(matched, unMatched, grouperBaseLine.unmatchedReportDS)

    calculationResults.append("BaseLine", baseLineRes)

    blMatchedDS
  }

  private def compareColumn(checkedColumn: String): Unit = {

    val grouper = GroupProcessor(baseLineMatchedDS, WallMartRow.baseLineCols :+ checkedColumn, RowInfo.sourceTextCols)

    val (matched, unmatched) = grouper.matchUnmatchNums

    calculationResults.append(checkedColumn, ColumnResult(matched, unmatched, grouper.unmatchedReportDS))
  }
}
