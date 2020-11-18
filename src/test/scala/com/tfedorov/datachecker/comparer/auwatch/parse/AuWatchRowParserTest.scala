package com.tfedorov.datachecker.comparer.auwatch.parse

import com.tfedorov.datachecker.comparer.auwatch.AuWatchRow
import com.tfedorov.datachecker.comparer.auwatch.AuWatchRow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class AuWatchRowParserTest {

  @Test
  def originalParsePccDesc(): Unit = {
    val emrLine = "HCUNVS20200427193415COVID19/PIZZA/FOOD/PEOPLE AT TABLE CASO CERRADO             GENERAL VARIETY               M-F EARLY FRINGE DOMINOS PIZZA INC                  DOMINOS PIZZA LLC                  DOMINOS PIZZA RESTAURANT-QUICK SVC                               RESTRNTS,HTL DINING&NIGHTCLUBS01500000000003004CNOS PIZZA RESTAURANT-QUICK SVC006"

    val result: AuWatchRow = Parser.originalParse(emrLine)

    assertEquals("RESTRNTS,HTL DINING&NIGHTCLUBS", result.pccDesc)
  }

  @Test
  def checkedParseParsedMaxSequenceWithinPod(): Unit = {
    val emrLine = "HCTUDN202005021640212 BOYS & GIRL ON COUCH/WOMAN/TEXT  CONTACTO DEPORTIVO L     SPORTS COMMENTARY             S&S AFTERNOON    T-MOBILE USA INC                   BOOST MOBILE LLC                   BOOST MOBILE UNLIMITED TELEPH SVCS-WIRELESS                      TELEPH COMPANIES,PUB & PRIVATE030 003003CWEB ACCESS                    013"

    val result: AuWatchRow = Parser.checkedParse(emrLine)

    assertEquals("013", result.maxSequenceWithinPod)
  }

  @Test
  def checkedParseParsedPccDesc(): Unit = {
    val emrLine = "HCTUDN202005021640212 BOYS & GIRL ON COUCH/WOMAN/TEXT  CONTACTO DEPORTIVO L     SPORTS COMMENTARY             S&S AFTERNOON    T-MOBILE USA INC                   BOOST MOBILE LLC                   BOOST MOBILE UNLIMITED TELEPH SVCS-WIRELESS                      TELEPH COMPANIES,PUB & PRIVATE030 003003CWEB ACCESS                    013"

    val result: AuWatchRow = Parser.checkedParse(emrLine)

    assertEquals("TELEPH COMPANIES,PUB & PRIVATE", result.pccDesc)
  }

  @Test
  def originalParseExpenditure(): Unit = {
    val emrLine = "HCGALA20200304062938WOMAN IN KITCHEN/PEOPLE AT TABLE   NINA DE MI CORAZON       DAYTIME DRAMA                 M-F NOON NEWS    NESTLE SA                          NESTLE BEVERAGE CO                 NESTLE LA LECHERA MILK                                           MILK,BUTTER,EGGS(INCL PWRD)   01500000056003002CCONDENSED                     011"

    val result: AuWatchRow = Parser.originalParse(emrLine)

    assertEquals("00000056", result.expenditure)
  }

  @Test
  def originalParseS_SOVERNIGHT(): Unit = {
    val emrLine = "NCHALL20200503035556WOMAN & GIRL ON COUCH/PRODUCTS/LOGOHALLMARK MOVIE           FEATURE FILM                  S-S OVERNIGHT    KAO CORP                           KAO USA INC                        JERGENS NATURAL GLOW MOISTURIZER-BODY                            SKIN CARE CREAMS,LOTIONS&OILS 01500000000004006C                              007"

    val result: AuWatchRow = Parser.originalParse(emrLine)

    assertEquals("S&S OVERNIGHT", result.dayPart)
  }

  @Test
  def checkedParseExpenditure(): Unit = {
    val emrLine = "HCGALA20200304062938WOMAN IN KITCHEN/PEOPLE AT TABLE   NINA DE MI CORAZON       DAYTIME DRAMA                 M-F EARLY MORNINGNESTLE SA                          NESTLE BEVERAGE CO                 NESTLE LA LECHERA MILK                                           MILK,BUTTER,EGGS(INCL PWRD)   01500055.50003002CCONDENSED                     011"

    val result: AuWatchRow = Parser.checkedParse(emrLine)

    assertEquals("00055.50", result.expenditure)
  }

}
