package com.tfedorov.datachecker.comparer.smi

import com.tfedorov.datachecker.comparer.smi.SMIRow.{baseLineCols, checkedCols}
import com.tfedorov.datachecker.comparer.{ColumnComparator, GroupProcessor, RowInfo}
import com.tfedorov.datachecker.{AppContext, CalculationResult, ColumnResult}
import org.apache.spark.sql._

case class SMIComparator(appArgs: AppContext)(implicit val session: SparkSession) extends ColumnComparator {

  import session.sqlContext.implicits._

  override val resolvedColumns: Seq[String] = appArgs.columns.split(",").toList match {
    case "_all" :: Nil => SMIRow.checkedCols
    case "_BaseLine" :: Nil => Nil
    case consoleColumns => consoleColumns
  }
  private var calculationResults: CalculationResult = CalculationResult.empty

  override def compareColumns(): CalculationResult = {
    baseLineMatchedDS.cache()
    resolvedColumns.foreach(compareColumn)
    calculationResults
  }

  private lazy val unionDS: Dataset[SMIRow] = {

    val originRDD = session.sparkContext.textFile(appArgs.originalInputPath).distinct()
    val originalDS: Dataset[SMIRow] = originRDD.map(Parser.originalParse).filter(_.isSuccess).map(_.get).toDS()

    val checkedRDD = session.sparkContext
      .textFile(appArgs.checkedInputPath).distinct()
    val checkedDS: Dataset[SMIRow] = checkedRDD.map(Parser.checkedParse).filter(_.isSuccess).map(_.get).toDS()

    calculationResults = CalculationResult(originalNumber = originalDS.count(), checkedNumber = checkedRDD.count())
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