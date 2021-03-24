package gov.nasa.test.loganalyzer

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.scalatest.FunSuite

import gov.nasa.loganalyzer.AccessLog
import gov.nasa.loganalyzer.InvokeNasaProcessor

class InvokeNasaProcessorTest extends FunSuite with SparkSessionTestWrapper {

  import spark.implicits._

  var numOfResultsToFetch: Int = _
  var filterResponseCodesSeq_test1: Seq[String] = _
  var filterResponseCodesSeq_test2: Seq[String] = _

  var rawDataWithHttp: Seq[String] = _
  var rawDataWithoutHttp: Seq[String] = _

  //Setup environment variables and data
  setup_test_env()

  //Create spark datasets from the input data
  val dataWithHttp = spark.createDataset(rawDataWithHttp)
  val dataWithoutHttp = spark.createDataset(rawDataWithoutHttp)

  //Generate AccessLog objects for the test input data
  val accessLogLinesWithHttp = setup_accessLogLinesWithHttp()
  val accessLogLinesWithoutHttp = setup_accessLogLinesWithoutHttp()

  val accessLogLines = accessLogLinesWithoutHttp.union(accessLogLinesWithHttp)

  val window = Window.partitionBy($"date").orderBy($"count".desc)

  //Get top N visitors against the test data
  val accessLogLinesTopVisitors = InvokeNasaProcessor.getTopNVisitors(
    accessLogLines,
    numOfResultsToFetch,
    window)
  //Get top N urls against the test data with filter codes set
  val accessLogLinesTopUrlsWithFilters = InvokeNasaProcessor.getTopNUrls(
    accessLogLines,
    numOfResultsToFetch,
    window,
    filterResponseCodesSeq_test1)
  //Get top N visitors against the test data with no filter codes set
  val accessLogLinesTopUrlsWithoutFilters = InvokeNasaProcessor.getTopNUrls(
    accessLogLines,
    numOfResultsToFetch,
    window,
    filterResponseCodesSeq_test2)

  //Test Scenarios: Begin
  test("Total row-count of getTopNVisitors") {
    assert(accessLogLinesTopVisitors.count() == 2)
  }

  test("Visit-count of topN visitors of getTopNVisitors") {
    assert(accessLogLinesTopVisitors.take(2)(0)(2) == 4)
    assert(accessLogLinesTopVisitors.take(2)(1)(2) == 2)
  }

  test("Visitor-record of top visitor of getTopNVisitors") {
    assert(accessLogLinesTopVisitors.take(1)(0)(0) == "129.48.125.52")
    assert(accessLogLinesTopVisitors.take(1)(0)(1) == "28/Jul/1995")
    assert(accessLogLinesTopVisitors.take(1)(0)(2) == 4)
  }

  test("Total row-count of getTopNUrls with responseCode filter disabled") {
    assert(accessLogLinesTopUrlsWithFilters.count() == 4)
  }

  test("Visit-count of topN urls of getTopNUrls with responseCode filter disabled") {
    assert(accessLogLinesTopUrlsWithFilters.take(4)(0)(2) == 2)
    assert(accessLogLinesTopUrlsWithFilters.take(4)(1)(2) == 2)
    assert(accessLogLinesTopUrlsWithFilters.take(4)(2)(2) == 1)
    assert(accessLogLinesTopUrlsWithFilters.take(4)(3)(2) == 1)
  }

  test("Visitor-record of top url of getTopNUrls with responseCode filter disabled") {
    assert(accessLogLinesTopUrlsWithFilters.take(1)(0)(0) == "/history/test/images/test-small.gif")
    assert(accessLogLinesTopUrlsWithFilters.take(1)(0)(1) == "28/Jul/1995")
    assert(accessLogLinesTopUrlsWithFilters.take(1)(0)(2) == 2)
  }

  test("Total row-count of getTopNUrls with responseCode filter enabled") {
    assert(accessLogLinesTopUrlsWithoutFilters.count() == 4)
  }

  test("Visit-count of topN urls of getTopNUrls with responseCode filter enabled") {
    assert(accessLogLinesTopUrlsWithoutFilters.take(4)(0)(2) == 2)
    assert(accessLogLinesTopUrlsWithoutFilters.take(4)(1)(2) == 2)
    assert(accessLogLinesTopUrlsWithoutFilters.take(4)(2)(2) == 2)
    assert(accessLogLinesTopUrlsWithoutFilters.take(4)(3)(2) == 1)
  }

  test("Visitor-record of top url of getTopNUrls with responseCode filter enabled") {
    assert(accessLogLinesTopUrlsWithoutFilters.take(1)(0)(0) == "/history/test/images/test-small.gif")
    assert(accessLogLinesTopUrlsWithoutFilters.take(1)(0)(1) == "28/Jul/1995")
    assert(accessLogLinesTopUrlsWithoutFilters.take(1)(0)(2) == 2)
  }
  //Test Scenarios: End

  //Setup method- create Dataset[AccessLog]
  def setup_accessLogLinesWithoutHttp(): Dataset[AccessLog] = {

    val filteredLogLinesWithoutHttp = dataWithoutHttp.map(_.split(" "))
      .map(attributes => (attributes.length, attributes))
    val accessLogLinesWithoutHttp = filteredLogLinesWithoutHttp.map(x =>
      AccessLog(
        x._2(0),
        x._2(3).substring(1, x._2(3).indexOf(":")),
        x._2(5).substring(1),
        x._2(6).substring(0, x._2(6).length - 1),
        x._2(7).trim))
    return accessLogLinesWithoutHttp

  }

  //Setup method- create Dataset[AccessLog]
  def setup_accessLogLinesWithHttp(): Dataset[AccessLog] = {
    val filteredLogLinesWithHttp = dataWithHttp.map(_.split(" "))
      .map(attributes => (attributes.length, attributes))
    val accessLogLinesWithHttp = filteredLogLinesWithHttp.map(x =>
      AccessLog(
        x._2(0),
        x._2(3).substring(1, x._2(3).indexOf(":")),
        x._2(5).substring(1),
        x._2(6).trim,
        x._2(8).trim))
    return accessLogLinesWithHttp

  }

  //Setup method- create input data and set config variables
  def setup_test_env() = {
    numOfResultsToFetch = 2
    filterResponseCodesSeq_test1 = Seq(("404"), ("400"))
    filterResponseCodesSeq_test2 = Seq()

    rawDataWithHttp = Seq(
      ("129.48.125.52 - - [28/Jul/1995:11:38:24 -0400] \"GET /history/test/images/test-small.gif HTTP/1.0\" 200 9630"),
      ("129.48.125.52 - - [28/Jul/1995:11:38:24 -0400] \"GET /images/newll.gif HTTP/1.0\" 200 1204"),
      ("n1123209.new.nasa.gov - - [28/Jul/1995:11:38:29 -0400] \"GET /images/newlogo-medium.gif HTTP/1.0\" 200 5866"))

    rawDataWithoutHttp = Seq(
      ("129.48.125.52 - - [28/Jul/1995:11:38:24 -0400] \"GET /history/test/images/test-small.gif\" 200 9630"),
      ("129.48.125.52 - - [28/Jul/1995:11:38:24 -0400] \"GET /images/newll.gif\" 404 1204")
      )

  }

}