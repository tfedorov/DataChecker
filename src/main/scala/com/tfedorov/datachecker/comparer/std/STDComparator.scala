package com.tfedorov.datachecker.comparer.std

import com.tfedorov.datachecker.comparer.{ColumnComparator, GroupProcessor, RowInfo}
import com.tfedorov.datachecker.utils.FileNameUtils.folderNameOpt
import com.tfedorov.datachecker.{AppContext, CalculationResult, ColumnResult}
import org.apache.spark.sql._

case class STDComparator(appArgs: AppContext)(implicit val session: SparkSession) extends ColumnComparator {

  import session.sqlContext.implicits._

  override val resolvedColumns: Seq[String] = appArgs.columns.split(",").toList match {
    case "_all" :: Nil => STDRowInfo.checkedCols
    case "_noPodCols" :: Nil => STDRowInfo._noPodCols
    case "_BaseLine" :: Nil => Nil
    case consoleColumns => consoleColumns
  }

  lazy val baseLineMatchedDS: Dataset[Row] = compareBaseLine(saveBaseLineRes)

  private var calculationResults: CalculationResult = CalculationResult.empty

  private def saveBaseLineRes: ColumnResult => Unit = calculationResults.append("BaseLine", _)

  private def saveRes(columnName: String): ColumnResult => Unit = calculationResults.append(columnName, _)


  override def compareColumns(): CalculationResult = {

    baseLineMatchedDS.cache()

    resolvedColumns.foreach { columnName =>
      compareColumn(columnName, saveRes(columnName))
    }
    calculationResults
  }

  private lazy val unionDS: Dataset[STDRowInfo] = {

    val originalDS: Dataset[STDRowInfo] = session
      .read
      .option("header", value = true).csv(appArgs.originalInputPath)
      .as[STDRow].map(_.toMFRowInfo)

    val checkedDS: Dataset[STDRowInfo] = session
      .read
      .option("header", value = true).csv(appArgs.checkedInputPath)
      .as[STDRow].map(_.toEMRRowInfo)

    calculationResults = CalculationResult(originalNumber = originalDS.count(), checkedNumber = checkedDS.count())
    originalDS.union(checkedDS).sort("NET_SH_NETWORK")
  }

  def compareBaseLine(resultProc: ColumnResult => Unit): Dataset[Row] = {
    val grouperBaseLine = GroupProcessor(unionDS, STDRowInfo.baseLineCols, STDRowInfo.checkedCols ++ RowInfo.sourceTextCols)

    val blMatchedDS: Dataset[Row] = grouperBaseLine.matchedRawDS
    val (matched, unMatched) = grouperBaseLine.matchUnmatchNums
    resultProc(ColumnResult(matched, unMatched, grouperBaseLine.unmatchedReportDS))
    blMatchedDS
  }


  def compareColumn(checkedColumn: String, resultProc: ColumnResult => Unit): Unit = {

    val grouper = GroupProcessor(baseLineMatchedDS, STDRowInfo.baseLineCols :+ checkedColumn, RowInfo.sourceTextCols)

    val (matched, unmatched) = grouper.matchUnmatchNums

    resultProc(ColumnResult(matched, unmatched, grouper.unmatchedReportDS))
  }

  private def compareColumnSQL(checkedColumn: String): Unit = {

    unionDS.createOrReplaceTempView("union")

    val groupedSQL =
      s"""
        SELECT NET_SH_NETWORK,
               $checkedColumn,
               count(TOT_NPG) AS matched,
               collect_list(originalSource) AS originalSource,
               collect_list(text) AS text
        FROM union
        GROUP BY NET_SH_NETWORK,
                 $checkedColumn
        ORDER BY NET_SH_NETWORK"""

    val groupedDS = session.sql(groupedSQL)
    groupedDS.cache()

    //groupedDS.show(100)

    groupedDS.createOrReplaceTempView("grouped")
    val detailedReportSQL =
      s"""SELECT NET_SH_NETWORK,
               $checkedColumn,
               originalSource,
               explode(text)
        FROM
          (SELECT NET_SH_NETWORK, $checkedColumn, explode(originalSource) AS originalSource, text
           FROM grouped
           WHERE matched == 1)"""
    val reportDS = session.sql(detailedReportSQL)

    reportDS.show(10)

    val matchedPerc = session.sql("SELECT * FROM grouped WHERE matched == 2").count
    val unmatchedPerc = calculationResults.originalNumber - matchedPerc
    calculationResults.append(checkedColumn, ColumnResult(matchedPerc, unmatchedPerc, reportDS))

    folderNameOpt(checkedColumn)(appArgs).foreach(reportDS.repartition(1).write.option("header", "true").csv(_))
  }


}