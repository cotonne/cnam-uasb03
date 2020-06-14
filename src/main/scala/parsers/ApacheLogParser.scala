package parsers

import config.Params
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter._

import models.ApacheLog
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ApacheLogParser {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[4]").setAppName("My Application")
    val sc = new SparkContext(config)

    val apacheLog: RDD[String] = sc.textFile(Params.PATH_TO_APACHE_LOG)

    val apacheLogRegex = ("(.*?) \\- (.*?) (\\[.*?\\]) \"(.*?)\" ([0-9]+?|-) (\\d+|-) \"(.*?)\" \"(.*?)\" \"(.*?)\"").r

    val apacheLogMessages = apacheLog map {
      l => {
        l match {
          case apacheLogRegex(remoteHost,
          remoteUser,
          timeReceived,
          requestFirstLine,
          status,
          responseBytes,
          requestHeaderReferer,
          requestHeaderUserAgent,
          remoteLogName) => ApacheLog(remoteHost,
            "??",
            remoteUser,
            LocalDateTime.parse(timeReceived, ISO_DATE_TIME),
            requestFirstLine,
            "",
            "",
            "",
            status.toInt,
            responseBytes.toInt,
            requestHeaderReferer,
            requestHeaderUserAgent,
            "",
            "",
            "",
            "",
            remoteLogName)
        }
      }
    }

    val content: RDD[ApacheLog] = apacheLogMessages
      .map(EnrichWithGeoIP.apply)
      .map(EnrichUserAgent.apply)
      .map(DecomposeURL.apply)

    content.saveAsTextFile(Params.PATH_TO_EXPORT_LOG)
  }
}
