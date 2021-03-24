package gov.nasa.loganalyzer

import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.JavaConverters
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.dense_rank
import org.apache.spark.sql.functions.not
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import org.apache.log4j.{Level, Logger}
import java.net.URL
import java.net.HttpURLConnection
import java.io.InputStream
import java.io.OutputStream
import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.io.IOException

import org.apache.spark.storage.StorageLevel

case class AccessLog(visitor: String, date: String, method: String, url: String, responseCode: String)
case class LogAnalysisException(severity: String, message: String, functionName: String) extends Exception(message: String)

object NasaStats {

  val logger = Logger.getLogger(this.getClass.getName())

  var configFilePath: String = ""
  var resultFileLoc: String = ""

  def main(args: Array[String]) = {

    //Build SparkSession and read input file
    val spark = SparkSession.builder()
      .appName("NasaStats")
      .getOrCreate()

    //Parse command-line-arguments
    parseInputArgs(args)
    logger.info("Parsed command-line-arguments.")

    //Read config file and set configuration properties
    val configfile = ConfigFactory.parseFile(new File(configFilePath))
    logger.info("Parsed config file- " + configFilePath)

    val numOfResultsToFetch = configfile.getInt("processor.numberOfResults.value")
    val isWriteCorruptLogFile = configfile.getBoolean("processor.writeCurruptLogEnteries.value")
    val filterResponseCodes = configfile.getStringList("processor.filterResponseCodes.value")
    val filterResponseCodesSeq = JavaConverters.asScalaIteratorConverter(filterResponseCodes.iterator())
      .asScala.toSeq
    //Read download URL if not present .
    val getDownLoadURL = configfile.getString("processor.downloadFileLoc.value")
    val gzFSLocation = configfile.getString("processor.gzFSLocation.value")

    if (numOfResultsToFetch <= 0) {
      logger.error("The numberOfResults property in the config is required to proceed further")
      System.exit(0);
    }
    if (gzFSLocation.length() <= 0) {
      logger.error("The file system directory to keep downloaded file is not defined in property file")
      System.exit(0);
    }

    if (!getDownLoadURL.equals("")) {
      if (getFile(getDownLoadURL, gzFSLocation) == 0) {
        logger.error("Exception : Exiting execution to error while downloading file.")
        System.exit(0)
      }
      logger.info("Download complete at location- " + gzFSLocation)
    } else {
      logger.info("Download not required. File is already at location- " + gzFSLocation)
    }

    val data = spark.sparkContext.textFile("file://"+gzFSLocation)
    logger.info("Spark loaded file from path- " + gzFSLocation)
    //To convert rdd to dataframes/datasets
    import spark.implicits._

    //Tokenize input lines using SPACE as tokenizer, make map of (tokens.size, tokens) and convert the rdd to dataset
    logger.info("Data processing begin... ")
    val logLinesDS = data.map(_.split(" "))
      .map(attributes => (attributes.length, attributes))
      .toDS()
    logLinesDS.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    //Filter logLinesDS based on tokens.size, i.e. attribute.length.
    val logLinesWithHttpDS = logLinesDS.filter(logLinesDS("_1") === 10)
    val logLinesWithoutHttpDS = logLinesDS.filter(logLinesDS("_1") === 9)

    //Filter corrupt logLinesDS entries with incomplete information for processing.
    if (isWriteCorruptLogFile) {
      val curruptLogLinesDS = logLinesDS.filter(logLinesDS("_1") < 9)
      val curruptLogLinesFormattedDS = curruptLogLinesDS.map(x => x._1 + " - " + (x._2).mkString(" "))
      InvokeNasaProcessor.writeCurruptLogLinesToFS(curruptLogLinesFormattedDS, resultFileLoc)
    }

    logLinesDS.unpersist()

    //Create AccessLog objects from the filteredLines with & without HTTP/1.0
    val accessLogLinesWithHttp = logLinesWithHttpDS.map(x =>
      AccessLog(
        x._2(0),
        x._2(3).substring(1, x._2(3).indexOf(":")),
        x._2(5).substring(1),
        x._2(6).trim,
        x._2(8).trim))
    val accessLogLinesWithoutHttp = logLinesWithoutHttpDS.map(x =>
      AccessLog(
        x._2(0),
        x._2(3).substring(1, x._2(3).indexOf(":")),
        x._2(5).substring(1),
        x._2(6).substring(0, x._2(6).length - 1),
        x._2(7).trim))
    val accessLogLines = accessLogLinesWithoutHttp.union(accessLogLinesWithHttp)
    accessLogLines.cache()
    logger.info("AccessLog lines prepared..")

    //Define window for ranking operation
    val window = Window.partitionBy($"date").orderBy($"count".desc)

    //Fetch topN visitors
    val accessLogLinesTopVisitorsDF = InvokeNasaProcessor.getTopNVisitors(accessLogLines, numOfResultsToFetch, window)
    logger.info("Top" + numOfResultsToFetch + " Visitors processed.")

    //Fetch topN urls
    val accessLogLinesTopUrlsDF = InvokeNasaProcessor.getTopNUrls(accessLogLines, numOfResultsToFetch, window, filterResponseCodesSeq)
    logger.info("Top" + numOfResultsToFetch + " urls processed.")

    accessLogLines.unpersist()

    //Write the output to FS
    InvokeNasaProcessor.writeResultsToFS(accessLogLinesTopVisitorsDF, accessLogLinesTopUrlsDF, resultFileLoc, numOfResultsToFetch)
    logger.info("Data processing finished. Output location- " + resultFileLoc)

    spark.stop()
  }

  //Function to parse command-line arguments and set local variables
  def parseInputArgs(args: Array[String]) = {
    if (args.length < 2) {
      logger.error("The application property file path and/or output directory path are missing in the argument list!")
      System.exit(0);
    } else {
      configFilePath = args(0)
      resultFileLoc = args(1)
    }
  }

  //Function to download the file from ftp
  def getFile(downloadURL: String, filePath: String): Int = {
    val url = new URL(downloadURL)
    var return_code = 1
    var in: InputStream = null
    var out: OutputStream = null

    try {
      val connection = url.openConnection()
      connection.setConnectTimeout(5000)
      connection.setReadTimeout(5000)
      connection.connect()
      logger.info("Connection to FTP server successfull.")

      in = connection.getInputStream
      val fileToDownloadAs = new java.io.File(filePath)
      out = new BufferedOutputStream(new FileOutputStream(fileToDownloadAs))
      logger.info("Downloading file from location- " + downloadURL)
      logger.info("Downloading file to location- " + filePath)

      val byteArray = Stream.continually(in.read).takeWhile(-1 !=).map(_.toByte).toArray
      out.write(byteArray)

    } catch {
      case ex: Exception =>
        {
          logger.error(new LogAnalysisException("error", s"getFile from ftp failed. Msg: $ex", "getFile").toString());
          return_code = 0
        }

    } finally {
      try {
        in.close()
        out.close()
      } catch {
        case ex: Exception => logger.error(new LogAnalysisException("error", s"Closing input/output streams. Msg: $ex", "getFile").toString());
      }

    }
    return return_code
  }
}