package com.tfedorov.datachecker.comparer.wallmart

import com.tfedorov.datachecker.comparer.RowInfo

import scala.util.Try

object Parser {

  def checkedParseTry(line: String): Try[WallMartRow] = Try(substringParse(line))

  def originalSubstring(line: String): WallMartRow = substringParse(line).copy(originalSource = RowInfo.MAINFRAME_SOURCE)

  def originalCsv(line: String): WallMartRow = csvParse(line).copy(originalSource = RowInfo.MAINFRAME_SOURCE)

  protected[wallmart] def csvParse(line: String): WallMartRow = {
    val splittedLine = line.split(",")
    if (splittedLine.length < 18)
      throw new RuntimeException(s"Could not parse '$line'")

    val marketCode: String = splittedLine(0).trim
    val callLetter: String = splittedLine(1).trim
    val affliationCode: String = splittedLine(2).trim
    val commercialDate: String = splittedLine(3).trim
    val commercialTime: String = splittedLine(4).trim
    val ultimateParentDesc: String = splittedLine(5).trim
    val brandDesc: String = splittedLine(6).trim
    val commercialDescription: String = splittedLine(7).trim
    val dayPart: String = splittedLine(8).trim
    val commercialDuration: String = splittedLine(9).trim
    val commercialType: String = splittedLine(10).trim
    val showType: String = splittedLine(11).trim
    val tvHouseHold: String = grpProcess(splittedLine(12).trim)
    val adult_18_49: String = grpProcess(splittedLine(13).trim)
    val women_18_49: String = grpProcess(splittedLine(14).trim)
    val adult_35_54: String = grpProcess(splittedLine(15).trim)
    val men_18_34: String = grpProcess(splittedLine(16).trim)
    val persons_6_17: String = grpProcess(splittedLine(17).trim)

    WallMartRow(
      marketCode,
      callLetter,
      affliationCode,
      commercialDate,
      commercialTime,
      ultimateParentDesc,
      brandDesc,
      commercialDescription,
      dayPart,
      commercialDuration,
      commercialType,
      showType,
      tvHouseHold,
      adult_18_49,
      women_18_49,
      adult_35_54,
      men_18_34,
      persons_6_17,
      //Extra columns metadata
      originalSource = RowInfo.MAINFRAME_SOURCE,
      line
    )
  }

  private[wallmart] def grpProcess: String => String = round2

  private[wallmart] def round2(input: String): String =
    BigDecimal(input).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble.toString


  private[wallmart] def grpTruncate(input: String): String = {
    val pointIndex = input.lastIndexOf(".")
    if (pointIndex < 0) return input

    if (input.length - pointIndex > 3)
      return input.substring(0, pointIndex + 3)
    input
  }

  private def substringParse(line: String): WallMartRow = {
    if (line.length < 187)
      throw new RuntimeException(s"Could not parse '$line'")

    val marketCode: String = line.substring(0, 2).trim
    val callLetter: String = line.substring(2, 6).trim
    val affliationCode: String = line.substring(6, 9).trim
    val commercialDate: String = line.substring(9, 17).trim
    val commercialTime: String = line.substring(17, 23).trim
    val ultimateParentDesc: String = line.substring(23, 58).trim
    val brandDesc: String = line.substring(58, 123).trim
    val commercialDescription: String = line.substring(123, 158).trim
    val dayPart: String = line.substring(158, 175).trim
    val commercialDuration: String = line.substring(175, 178).trim
    val commercialType: String = line.substring(178, 179).trim
    val showType: String = line.substring(179, 181).trim
    if (line.length <= 187) {
      val tvHouseHold: String = ""
      val adult_18_49: String = ""
      val women_18_49: String = ""
      val adult_35_54: String = ""
      val men_18_34: String = ""
      val persons_6_17: String = ""

      WallMartRow(
        marketCode,
        callLetter,
        affliationCode,
        commercialDate,
        commercialTime,
        ultimateParentDesc,
        brandDesc,
        commercialDescription,
        dayPart,
        commercialDuration,
        commercialType,
        showType,
        tvHouseHold,
        adult_18_49,
        women_18_49,
        adult_35_54,
        men_18_34,
        persons_6_17,
        //Extra columns metadata
        originalSource = RowInfo.EMR_SOURCE,
        line
      )
    } else {
      val tvHouseHold: String = grpProcess(line.substring(181, 189).trim)
      val adult_18_49: String = grpProcess(line.substring(189, 197).trim)
      val women_18_49: String = grpProcess(line.substring(197, 205).trim)
      val adult_35_54: String = grpProcess(line.substring(205, 213).trim)
      val men_18_34: String = grpProcess(line.substring(213, 221).trim)
      val persons_6_17: String = grpProcess(line.substring(221).trim)

      WallMartRow(
        marketCode,
        callLetter,
        affliationCode,
        commercialDate,
        commercialTime,
        ultimateParentDesc,
        brandDesc,
        commercialDescription,
        dayPart,
        commercialDuration,
        commercialType,
        showType,
        tvHouseHold,
        adult_18_49,
        women_18_49,
        adult_35_54,
        men_18_34,
        persons_6_17,
        //Extra columns metadata
        originalSource = RowInfo.EMR_SOURCE,
        line
      )
    }
  }
}
