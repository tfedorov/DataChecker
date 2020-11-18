package com.tfedorov.datachecker.comparer.smi

import com.tfedorov.datachecker.comparer.RowInfo
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ParserTest {

  @Test
  def originalParse(): Unit = {
    val input = "\"MUN2 CABLE\" \"2020-04-12 22:40:04\" \"015\" \"PRIME TIME\" \"SUNDAY\" \"LARCHE GREEN NV\" \"HEINEKEN USA INC\" \"TECATE BEER\" \"BEER\" \"BEER & WINE\" \"BEER, WINE, & LIQUOR\" \"BEER\" \"\" \"\" \"005\" \"009\" \"HOLLYWOOD MED TYLER S2\" \"2020-04-12 22:00:00\" \"060\" \"GENERAL DRAMA\" \"\" \"R\" \"N\" \"R\" \"N\" \"\"                                                                                                                                                                                                                 "

    val actual: SMIRow = Parser.originalParse(input).get

    assertEquals("MUN2 CABLE", actual.STATION)
    /*
    assertEquals("2020-04-13 00:11:31", actual.CMCL_DTM)
    assertEquals("015", actual.LEN)
    assertEquals("LATE FRINGE", actual.DAYPART)
    assertEquals("SUNDAY", actual.BROADCAST_DAY)
    assertEquals("PROMO", actual.ADVERTISER)
    assertEquals("PROMO", actual.SUBSIDIARY)
    assertEquals("ADULT SWIM THREE BUSY DEBRAS TV PGM-CABLE-ENT", actual.BRAND)
    assertEquals("TV PGM-CABLE-ENT", actual.PRODUCT)
    assertEquals("TV PROGRAMS", actual.INDUSTRY)
    assertEquals("TV PROGRAMS-CABLE", actual.MAJOR)
    assertEquals("TV PGM-CABLE-ENT", actual.PCC_SUB)
    assertEquals("", actual.IS_PROMO)
    assertEquals("", actual.IS_DR)
    assertEquals("001", actual.POD)
    assertEquals("001", actual.POS)
    assertEquals("THREE BUSY DEBRAS", actual.SHOW_TITLE)
    assertEquals("2020-04-13 00:00:00", actual.SHOW_DTM)
    assertEquals("015", actual.SHOW_LEN)
    assertEquals("SITUATION COMEDY", actual.PROGRAM_TYPE)
    assertEquals("", actual.PROGRAM_SUB_TYPE)
    assertEquals("R", actual.AIR_TYPE)
    assertEquals("N", actual.RUN_TYPE)
    assertEquals("R", actual.EPISODE_TYPE)
    assertEquals("N", actual.EVENT_TYPE)
    assertEquals("", actual.SYNDICATION)
     */
    assertEquals(input, actual.text)
    assertEquals(RowInfo.MAINFRAME_SOURCE, actual.originalSource)
  }


  @Test
  def checkedParse(): Unit = {
    val input = "\"ABC\" \"2020-04-09 03:45:26\" \"030\" \"M-F OVERNIGHT\" \"THURSDAY\" \"PROCTER & GAMBLE CO\" \"PROCTER & GAMBLE CO\" \"BOUNTY PAPER TOWELS\" \"PAPER TOWELS\" \"HOUSEHOLD EQUIPMENT & SUPPLIES\" \"HOUSEHOLD ACCESSORIES & MISCELLANEOUS SUPPLIES\" \"HOUSEHOLD PAPER PRODUCTS\" \"\" \"\" \"010\" \"001\" \"ABC WORLD NEWS NOW\" \"2020-04-08 02:37:00\" \"083\" \"NEWS\" \"NEWS-GENERAL\" \"R\" \"N\" \"R\" \"N\" \"\""

    val actual: SMIRow = Parser.checkedParse(input).get

    assertEquals("ABC", actual.STATION)
    assertEquals("2020-04-09 03:45:26", actual.CMCL_DTM)
    assertEquals("030", actual.LEN)
    assertEquals("M-F OVERNIGHT", actual.DAYPART)
    assertEquals("THURSDAY", actual.BROADCAST_DAY)
    assertEquals("PROCTER & GAMBLE CO", actual.ADVERTISER)
    assertEquals("PROCTER & GAMBLE CO", actual.SUBSIDIARY)
    assertEquals("BOUNTY PAPER TOWELS", actual.BRAND)
    assertEquals("PAPER TOWELS", actual.PRODUCT)
    assertEquals("HOUSEHOLD EQUIPMENT & SUPPLIES", actual.INDUSTRY)
    assertEquals("HOUSEHOLD ACCESSORIES & MISCELLANEOUS SUPPLIES", actual.MAJOR)
    assertEquals("HOUSEHOLD PAPER PRODUCTS", actual.PCC_SUB)
    assertEquals("", actual.IS_PROMO)
    assertEquals("", actual.IS_DR)
    assertEquals("010", actual.POD)
    assertEquals("001", actual.POS)
    assertEquals("ABC WORLD NEWS NOW", actual.SHOW_TITLE)
    assertEquals("2020-04-08 02:37:00", actual.SHOW_DTM)
    assertEquals("083", actual.SHOW_LEN)
    assertEquals("NEWS", actual.PROGRAM_TYPE)
    assertEquals("NEWS-GENERAL", actual.PROGRAM_SUB_TYPE)
    assertEquals("R", actual.AIR_TYPE)
    assertEquals("N", actual.RUN_TYPE)
    assertEquals("R", actual.EPISODE_TYPE)
    assertEquals("N", actual.EVENT_TYPE)
    assertEquals("", actual.SYNDICATION)

    assertEquals(input, actual.text)
    assertEquals(RowInfo.EMR_SOURCE, actual.originalSource)
  }

}
