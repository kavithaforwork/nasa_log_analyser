package gov.nasa.loganalyzer

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.dense_rank
import org.apache.spark.sql.functions.not

object InvokeNasaProcessor {

  val logger = Logger.getLogger(this.getClass.getName())

  //Function to get current timestamp
  def getNow(): String = {
    val nowFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val now = nowFormat.format(Calendar.getInstance().getTime())
    logger.debug("Output files postfix generated- " + now)
    return now
  }

  //Function to get top N visitors
  def getTopNVisitors(
    accessLogLines: Dataset[AccessLog],
    numOfResultsToFetch: Int,
    window: WindowSpec): Dataset[Row] = {
    logger.debug("getTop " + numOfResultsToFetch + " Visitors started.")

    val accessLogLinesGroupByVisitor = accessLogLines.groupBy("visitor", "date")
      .count()
    val accessLogLinesRankedForVisitors = accessLogLinesGroupByVisitor.withColumn("dense_rank", dense_rank over window)
      .filter(col("dense_rank") <= numOfResultsToFetch)

    val accessLogLinesTopVisitors = accessLogLinesRankedForVisitors.select("*")
      .orderBy(col("date").asc, col("count").desc)
    logger.debug("getTop " + numOfResultsToFetch + " Visitors finished.")
    return accessLogLinesTopVisitors

  }

  //Function to get top N urls
  def getTopNUrls(
    accessLogLines: Dataset[AccessLog],
    numOfResultsToFetch: Int,
    window: WindowSpec,
    filterResponseCodesSeq: Seq[String]): Dataset[Row] = {

    logger.debug("getTop " + numOfResultsToFetch + " Urls started.")

    var accessLogLinesGroupByUrl: org.apache.spark.sql.DataFrame = null;

    if (filterResponseCodesSeq.size > 0) {
      logger.debug("responseCode filtering is enabled when retrieving top  " + numOfResultsToFetch + " urls. The resopnseCodes are- " + filterResponseCodesSeq)
      accessLogLinesGroupByUrl = accessLogLines.filter(not(col("responseCode") isin (filterResponseCodesSeq: _*)))
        .groupBy("url", "date")
        .count()
    } else {
      logger.debug("responseCode filtering is disabled when retrieving top  " + numOfResultsToFetch + " urls.")
      accessLogLinesGroupByUrl = accessLogLines.groupBy("url", "date")
        .count()
    }
    val accessLogLinesRankedForUrl = accessLogLinesGroupByUrl.withColumn("dense_rank", dense_rank over window)
      .filter(col("dense_rank") <= numOfResultsToFetch)
    val accessLogLinesTopUrls = accessLogLinesRankedForUrl.select("*")
      .orderBy(col("date").asc, col("count").desc)
    logger.debug("getTop " + numOfResultsToFetch + " Urls finished.")
    return accessLogLinesTopUrls

  }

  //Function to write the results on FileSystem
  def writeResultsToFS(
    accessLogLinesTopVisitors: Dataset[Row],
    accessLogLinesTopUrls: Dataset[Row],
    resultFileLoc: String,
    numOfResultsToFetch: Int) = {

    logger.debug("Write results to filesystem started.")
    accessLogLinesTopVisitors.coalesce(1)
      .write.option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv(path = "file://"+resultFileLoc + "nasa_top_" + numOfResultsToFetch + "_visitors_" + System.currentTimeMillis())
    accessLogLinesTopUrls.coalesce(1)
      .write
      .option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv("file://"+resultFileLoc + "nasa_top_" + numOfResultsToFetch + "_urls_" + System.currentTimeMillis())
    logger.debug("Write results to filesystem finished.")
  }

  //Function to write the corrupt log lines on FileSystem
  def writeCurruptLogLinesToFS(
    curruptLogLinesFormatted: Dataset[String],
    resultFileLoc: String) = {
    logger.debug("Write corrupt entried to filesystem started.")
    curruptLogLinesFormatted.coalesce(1)
      .write.option("header", "true")
      .mode("overwrite")
      .csv("file://"+resultFileLoc + "nasa_access_log_currupt_enteries_" + System.currentTimeMillis())
    logger.debug("Write corrupt entried to filesystem finished.")

  }

}