package com.tfedorov.datachecker.comparer.auwatch

import com.tfedorov.datachecker.comparer.auwatch.parse.Parser
import com.tfedorov.datachecker.comparer.{ColumnComparator, GroupProcessor, RowInfo}
import com.tfedorov.datachecker.{AppContext, CalculationResult, ColumnResult}
import org.apache.spark.sql._
import com.tfedorov.datachecker.comparer._
import com.tfedorov.datachecker.comparer.auwatch.AuWatchRow.{baseLineCols, checkedCols}
import com.tfedorov.datachecker.{AppContext, CalculationResult, ColumnResult, comparer}

case class AuWatchComparator(appArgs: AppContext)(implicit val session: SparkSession) extends ColumnComparator {

  import session.sqlContext.implicits._

  override val resolvedColumns: Seq[String] = appArgs.columns.split(",").toList match {
    case "_all" :: Nil => AuWatchRow.checkedCols
    case consoleColumns => consoleColumns
  }

  private var calculationResults: CalculationResult = CalculationResult.empty

  override def compareColumns(): CalculationResult = {
    baseLineMatchedDS.cache()

    resolvedColumns.foreach(compareColumn)

    calculationResults
  }

  private lazy val unionDS: Dataset[AuWatchRow] = {

    val originRDD = session.sparkContext
      .textFile(appArgs.originalInputPath).distinct()
    val originalDS: Dataset[AuWatchRow] = originRDD.map[AuWatchRow](Parser.originalParse).toDS()

    val checkedRDD = session.sparkContext
      .textFile(appArgs.checkedInputPath).distinct()
    val checkedDS: Dataset[AuWatchRow] = checkedRDD.map(Parser.checkedParse).toDS()

    calculationResults = CalculationResult(originalNumber = originRDD.count(), checkedNumber = checkedRDD.count())
    originalDS.union(checkedDS)
  }

  private lazy val baseLineMatchedDS: Dataset[Row] = {
    val grouperBaseLine = GroupProcessor(unionDS, baseLineCols, checkedCols ++ RowInfo.sourceTextCols)

    val blMatchedDS = grouperBaseLine.matchedRawDS

    val (matched, unMatched) = grouperBaseLine.matchUnmatchNums

    val baseLineRes = ColumnResult(matched, unMatched, grouperBaseLine.unmatchedReportDS)

    calculationResults.append("BaseLine", baseLineRes)

    blMatchedDS.cache()
    blMatchedDS
  }

  private def compareColumn(checkedColumn: String): Unit = {

    val grouper = GroupProcessor(baseLineMatchedDS /*.where($"marketCode" =!= "HC")*/ , baseLineCols :+ checkedColumn, RowInfo.sourceTextCols)

    val (matched, unmatched) = grouper.matchUnmatchNums

    calculationResults.append(checkedColumn, ColumnResult(matched, unmatched, grouper.unmatchedReportDS))
  }
}
