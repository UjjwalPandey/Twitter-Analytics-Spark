package com.twitter.ujjwal

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object PopularHashTags {
  def getPopularHashTags(ssc : StreamingContext) {

    val CHECKPOINT_DESTINATION = "/home/manisha/IdeaProjects/Twitter-Analytics(Spark)/src/main/resource/PopularHashtags/"
//    val config: Config = ConfigFactory.load("TwitterProject.conf")
    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)

    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText)

    // Blow out each word into a new DStream
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))

    // Now eliminate anything that's not a hashtag
    val hashtags = tweetwords.filter(word => word.startsWith("#"))

    // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them by adding up the values
    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))

    // Now count them up over a 5 minute window sliding every one second
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(1))

    // Sort the results by the count values
    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, ascending = false))

    // Print the top 10
    sortedResults.print

    // Set a checkpoint directory, and kick it all off
    // I can watch this all day!

    ssc.checkpoint(CHECKPOINT_DESTINATION)

    ssc.start()

    ssc.awaitTermination()
  }
}
