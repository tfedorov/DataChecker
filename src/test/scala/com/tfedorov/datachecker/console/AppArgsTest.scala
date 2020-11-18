package com.tfedorov.datachecker.console

//import com.nielsen.media.adintel.datachecker.AppContext
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.util.{Success, Try}

class AppArgsTest {
/*
  @Test
  def applyArgsNoOut(): Unit = {
    val input = Array(
      "--original", "s3://us-east-1-nlsn-watch-adintel-exports-qa/stdextracts/CustomExtracts/MainFrame/PH220118.NATL.TXT",
      "--checked", "s3://us-east-1-nlsn-watch-adintel-exports-qa/stdextracts/CustomExtracts/MainFrame/PH220118.NATL.TXT",
      "--parser", "AuWatch",
      "--columns", "callLetter,commercialDescription,showTitle"
    )

    val result: Try[AppContext] = Try(AppContext.apply(input))

    val expected = Success(
      AppContext(
        "s3://us-east-1-nlsn-watch-adintel-exports-qa/stdextracts/CustomExtracts/MainFrame/PH220118.NATL.TXT",
        "s3://us-east-1-nlsn-watch-adintel-exports-qa/stdextracts/CustomExtracts/MainFrame/PH220118.NATL.TXT",
        "AuWatch",
        "callLetter,commercialDescription,showTitle",
        None
      )
    )
    assertEquals(expected, result)
  }

  @Test
  def applyArgsOut(): Unit = {
    val input = Array(
      "--original", "s3://us-east-1-nlsn-watch-adintel-exports-qa/stdextracts/CustomExtracts/MainFrame/PH220118.NATL.TXT",
      "--checked", "s3://us-east-1-nlsn-watch-adintel-exports-qa/stdextracts/CustomExtracts/MainFrame/PH220118.NATL.TXT",
      "--parser", "AuWatch",
      "--columns", "callLetter,commercialDescription,showTitle",
      "--output", "s3://us-east-1-nlsn-watch-adintel-exports-qa/stdextracts/CustomExtracts/Uniques-3-AuWatch-July2"
    )

    val result: Try[AppContext] = Try(AppContext.apply(input))

    val expected = Success(
      AppContext(
        "s3://us-east-1-nlsn-watch-adintel-exports-qa/stdextracts/CustomExtracts/MainFrame/PH220118.NATL.TXT",
        "s3://us-east-1-nlsn-watch-adintel-exports-qa/stdextracts/CustomExtracts/MainFrame/PH220118.NATL.TXT",
        "AuWatch",
        "callLetter,commercialDescription,showTitle",
        Some("s3://us-east-1-nlsn-watch-adintel-exports-qa/stdextracts/CustomExtracts/Uniques-3-AuWatch-July2")
      )
    )
    assertEquals(expected, result)
  }

  @Test
  def applyArgsFailed(): Unit = {
    val input = Array(
      "s3://us-east-1-nlsn-watch-adintel-exports-qa/stdextracts/CustomExtracts/MainFrame/PH220118.NATL.TXT",
      "s3://us-east-1-nlsn-watch-adintel-exports-qa/stdextracts/CustomExtracts/MainFrame/PH220118.NATL.TXT",
      "AuWatch",
      "callLetter,commercialDescription,showTitle",
      "s3://us-east-1-nlsn-watch-adintel-exports-qa/stdextracts/CustomExtracts/Uniques-3-AuWatch-July2"
    )

    val result: Try[AppContext] = Try(AppContext.apply(input))

    assertTrue(result.isFailure)
  }

  @Test
  def implicitsSTD(): Unit = {
    import AppContext.implicits._
    val inputOrigin = "/Users/tfedorov/IdeaProjects/docs/test_data/STNDCLTR/STD11604/MF_national.csv"
    val originOrigin = "/Users/tfedorov/IdeaProjects/docs/test_data/STNDCLTR/STD11604/EMR_national.csv"
    val input: (String, String) = (inputOrigin, originOrigin)

    val actualResult: AppContext = input.std

    val expected = AppContext(inputOrigin, originOrigin, "std", "_all", None)
    assertEquals(expected, actualResult)
  }
  */

}
