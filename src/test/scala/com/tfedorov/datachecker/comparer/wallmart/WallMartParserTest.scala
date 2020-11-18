package com.tfedorov.datachecker.comparer.wallmart

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.util.Try

class WallMartParserTest {

  @Test
  def csvParse(): Unit = {
    val mainFrameLine = "AB,WALB,NBC,20200127,102349,WALGREENS BOOTS ALLIANCE INC       ,WALGREENS STORE-DRUG                                             ,MAN IN GLASSES/WOMAN PLAYING GOLF  ,M-F LATE MORN    ,030,N,N ,004.4736,000.7780,001.1121,001.2043,000.2744,000.1893,"

    val result = Parser.csvParse(mainFrameLine)

    val expected = WallMartRow(
      marketCode = "AB",
      callLetter = "WALB",
      affliationCode = "NBC",
      commercialDate = "20200127",
      commercialTime = "102349",
      ultimateParentDesc = "WALGREENS BOOTS ALLIANCE INC",
      brandDesc = "WALGREENS STORE-DRUG",
      commercialDescription = "MAN IN GLASSES/WOMAN PLAYING GOLF",
      dayPart = "M-F LATE MORN",
      commercialDuration = "030",
      commercialType = "N",
      showType = "N",
      tvHouseHold = "4.47",
      adult_18_49 = "0.78",
      women_18_49 = "1.11",
      adult_35_54 = "1.2",
      men_18_34 = "0.27",
      persons_6_17 = "0.19",
      //Extra columns metadata
      originalSource = "Mainframe",
      text = mainFrameLine
    )
    assertEquals(expected, result)
  }

  @Test
  def isQuartOfHourFalse(): Unit = {
    val mainFrameLine = "AB,WALB,NBC,20200127,102349,WALGREENS BOOTS ALLIANCE INC       ,WALGREENS STORE-DRUG                                             ,MAN IN GLASSES/WOMAN PLAYING GOLF  ,M-F LATE MORN    ,030,N,N ,004.4736,000.7780,001.1121,001.2043,000.2744,000.1893,"

    val parsed: WallMartRow = Parser.csvParse(mainFrameLine)
    val actual = parsed.isQuartOfHour

    assertFalse(actual)
  }

  @Test
  def isQuartOfHourTrue(): Unit = {
    val mainFrameLine = "AB,WALB,NBC,20200127,100049,WALGREENS BOOTS ALLIANCE INC       ,WALGREENS STORE-DRUG                                             ,MAN IN GLASSES/WOMAN PLAYING GOLF  ,M-F LATE MORN    ,030,N,N ,004.4736,000.7780,001.1121,001.2043,000.2744,000.1893,"

    val parsed: WallMartRow = Parser.csvParse(mainFrameLine)
    val actual = parsed.isQuartOfHour

    assertTrue(actual)
  }

  @Test
  def checkedParse(): Unit = {
    val emrLine = "ABWALBNBC20160328081330MACYS INC                          MACYS STORE-DEPT                                                 DIAMOND SALE/GREAT SHOE SALE/COUPLEM-F EARLY MORN   030NN 011.6508003.4476004.7632005.6054001.1443000.1311"

    val result = Parser.checkedParseTry(emrLine)

    val expected = WallMartRow(
      marketCode = "AB",
      callLetter = "WALB",
      affliationCode = "NBC",
      commercialDate = "20160328",
      commercialTime = "081330",
      ultimateParentDesc = "MACYS INC",
      brandDesc = "MACYS STORE-DEPT",
      commercialDescription = "DIAMOND SALE/GREAT SHOE SALE/COUPLE",
      dayPart = "M-F EARLY MORN",
      commercialDuration = "030",
      commercialType = "N",
      showType = "N",
      tvHouseHold = "11.65",
      adult_18_49 = "3.45",
      women_18_49 = "4.76",
      adult_35_54 = "5.61",
      men_18_34 = "1.14",
      persons_6_17 = "0.13",
      //Extra columns metadata
      originalSource = "EMR",
      text = emrLine
    )
    assertEquals(expected, result.get)
  }

  @Test
  def checkedParseTryShort(): Unit = {
    val emrLine = "BPKFDMCBS20200415195933TARGET CORP                        TARGET GOOD & GATHER FOOD PDTS                                   "

    val result: Try[WallMartRow] = Parser.checkedParseTry(emrLine)

    assertTrue(result.isFailure)
  }

  @Test
  def checkedParseTryShort2(): Unit = {
    val emrLine = "BPKFDMCBS20200416000918TARGET CORP                        TARGET STORE-DEPT                                                INCOMPLETE VIDEO                   M-S LATE NIGHT   015NGV"

    val result: Try[WallMartRow] = Parser.checkedParseTry(emrLine)

    assertTrue(result.isFailure)
  }

  @Test
  def originalWMT19329(): Unit = {
    val emrLine = "ABWALBNBC20191125064204PUBLIX SUPER MARKETS INC           PUBLIX SUPERMARKET                                               FAMILY IN KITCHEN/PEOPLE AT TABLE  M-F EARLY MORN   060LN 009.4918002.2030002.7129003.4791001.2022000.6535"

    val result: WallMartRow = Parser.originalSubstring(emrLine)

    assertEquals("AB", result.marketCode)
    assertEquals("M-F EARLY MORN", result.dayPart)
    assertEquals("0.65", result.persons_6_17)
  }

  @Test
  def grpTest(): Unit = {
    val tvHouseHold = "011.6508"

    val result: String = Parser.grpProcess(tvHouseHold)

    assertEquals("11.65", result)
  }

  @Test
  def grpPlusTest(): Unit = {
    val adult_18_49 = "000.7780"

    val result: String = Parser.grpProcess(adult_18_49)

    assertEquals("0.78", result)
  }
}
