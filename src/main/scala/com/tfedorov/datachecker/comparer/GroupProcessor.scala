package com.tfedorov.datachecker.comparer

import com.tfedorov.datachecker.sparkCustom.ReportUDFs._
import org.apache.spark.sql.functions.{abs, explode, sum}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

case class GroupProcessor(target: Dataset[_], groupedCols: Seq[String], savedCols: Seq[String])
                         (implicit val session: SparkSession) {

  import session.sqlContext.implicits._

  private lazy val groupDS: DataFrame = {

    val groupedByColumnsDS = target.groupBy(groupedCols.head, groupedCols.tail: _*)

    val resGroupDS = groupedByColumnsDS
      .agg(cols2List(savedCols).as("_aggregatedData"))
      .withColumn("_matched", matchedUDF($"_aggregatedData"))
      .withColumn("_unMatched", unMatchedUDF($"_aggregatedData"))
    //.withColumn("_minus", minusExpenditureUDF($"_aggregatedData"))
    resGroupDS.cache()
    resGroupDS
  }

  private lazy val explodedDS: Dataset[Row] = {
    val explodedDS = groupDS.withColumn("_exploded", explode($"_aggregatedData"))

    extractSavedCols(explodedDS)
  }

  private def extractSavedCols(explodedDS: DataFrame): Dataset[Row] = {
    var resultDS: Dataset[Row] = explodedDS
    savedCols.foreach { column2Extract =>
      val columnExtractorUDF = colExtractUDF(column2Extract)
      resultDS = resultDS.withColumn(column2Extract, columnExtractorUDF($"_exploded"))
    }
    //resultDS.show
    resultDS
  }

  lazy val matchedRawDS: Dataset[Row] = explodedDS.filter($"_matched" =!= 0)

  lazy val unmatchedRawDS: Dataset[Row] = explodedDS.filter($"_unMatched" =!= 0)

  lazy val unmatchedReportDS: Dataset[Row] = {
    val reportColumns = groupedCols ++ RowInfo.sourceTextCols
    unmatchedRawDS.select(reportColumns.head, reportColumns.tail: _*)
  }

  lazy val matchUnmatchNums: (Long, Long) = {

    val matchedNum = longOrMinus(groupDS.agg(sum($"_matched")).head())
    val unMatchedNum = longOrMinus(groupDS.agg(sum(abs($"_unMatched"))).head())
    (matchedNum, unMatchedNum)
  }

  private def longOrMinus(row: Row): Long = if (row.isNullAt(0)) -1L else row.getLong(0)

}
