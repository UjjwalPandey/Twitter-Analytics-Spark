package com.twitter.ujjwal

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.io.Source

object Driver {
  val TWITTER_CREDENTIALS_PATH = "XXXXXXXX"

  def setupLogging(): Unit = {
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
  }

  def setupTwitter(): Unit = {
    val source = Source.fromFile(TWITTER_CREDENTIALS_PATH).getLines
    for (line <- source) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }

  }

  def main(args: Array[String]): Unit = {
    // Configure Twitter credentials using com.twitter.txt
    setupTwitter()
    // Set up a Spark streaming context named "PopularHashtags" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))   //  Enable when working through spark-submit
    // val ssc = new StreamingContext(sc, Seconds(1)) //  Enable when working on Spark-shell

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    PopularHashTags.getPopularHashTags(ssc)
  }
}
