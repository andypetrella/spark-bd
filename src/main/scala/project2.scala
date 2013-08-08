package be.bigdata.p2

import scala.collection.JavaConversions._

import akka.actor.{Actor, ActorRef, ActorSystem, Props}

import spark.streaming.{Seconds, StreamingContext}
import spark.streaming.StreamingContext._
import spark.SparkContext._


object Tick
case class For(actor:ActorRef, stocks:Seq[String])

case class Stock(
  id:String,
  keywords:Seq[String]
)

object Stocks {
  private[this] val seq = Seq(
    Stock("GOOG", Seq("google", "android")),
    Stock("APPL", Seq("apple", "ios"))
  )

  val defaults = seq.map(x => (x.id, x)).toMap
}

object P2 extends App {
  //prepare auth to twitter
  val twitterAuth = conf.root.getConfig("twitter.oauth")

  //init spark
  implicit val ssc = new StreamingContext("local", "Project2", Seconds(5))

  val topics = args.drop(1).toSeq.map{ t =>
    Stocks.defaults.get(t).getOrElse(Stock(t, Seq.empty))
  }

  lazy val twitter = new Twitter(twitterAuth)
  lazy val twitterDStream = twitter(topics.map(_.id)) // FIXME: only take the id for now

  lazy val yahoo = new Yahoo(spark.SparkAkka.urlFor("FeederActor"))
  lazy val yahooDStream = yahoo(topics.map(_.id)) // FIXME: only take the id for now

  if (args(0) == "yahoo") {
    Yahoo.start
    yahooDStream.foreach { rdd =>
      rdd.foreach { x =>
        println(x.toString)
      }
    }
  }

  ssc.start()



//map tweets to their sentiment score
//  val statusWithSentiment = stream.map{ status =>
//    val text = status.getText.toLowerCase
//    val (score, words) = sentiments.foldLeft((0, List():List[String])) { case ((score, ss), (w, s)) =>
//      if (text == w
//          || text.startsWith(w + " ")
//          || text.endsWith(" " + w)
//          || text.contains(" " + w + " ")) {
//        (score  + s, w :: ss)
//      } else {
//        (score, ss)
//      }
//    }
//    (score, words, status)
//  }
//
//  val topSentimental60sec =
//    statusWithSentiment
//      .map{ case (x, y, z) =>
//        (x, List((z,y)))
//      }

//  val score60secGroupedByTopic =
//    topSentimental60sec
//      .filter { case (score, (status, words) :: Nil) => !words.isEmpty}
//      .flatMap { case (score, (status, words) :: Nil) =>
//        //produce one element by topic included in the status
//        // in the case where the status matches several topics...
//        topics
//          .filter(topic => status.getText.contains("#"+topic))
//          .map(topic => (topic, List((status, words, score))))
//      }
//      .reduceByKeyAndWindow(_ ::: _, Seconds(60))
//      //for each topic => accScore, participatingTweets, allParticipatingSentiments
//      .mapValues(xs => (xs.map(_._3).sum, xs.map(_._1), xs.flatMap(_._2)))



//  val orderedTopSentimental60sec = topSentimental60sec
//                                    .reduceByKeyAndWindow(_ ::: _, Seconds(60))
//                                    .transform(_.sortByKey(false))

  // Print most sentimental for every topics
  //orderedTopSentimental60sec.foreach(rdd => {
  //  val topList = rdd.take(5)
  //  println("\nMost sentimental tweets in last 60 seconds (%s total):".format(rdd.count()))
  //  topList
  //    .foreach{case (sentiment, statuses) =>
  //      println("SENTIMENT :: " + sentiment)
  //      println("TWEETS :: ")
  //      statuses.map(x => "   " + x._1.getText + " |>>| " + x._2.mkString("|@|")).foreach(println)
  //      println("|||||||||||||||||||||||||||||||||||||||||||||||||||||")
  //    }
  //})

  // Print most sentimental for every topics
 // score60secGroupedByTopic
 //   .saveAsTextFiles("scoreByTopic", "60sec")

  //.foreach(rdd => {
  //  println("\nChange in sentiments by topic in last 60 seconds (%s total):".format(rdd.count()))
  //  rdd
  //    .foreach{case (topic, (score, tweets, words)) =>
  //      println("TOPIC :: " + score)
  //      println("TWEETS :: ")
  //      tweets.foreach(t => println(t.getText))
  //      println("WORDS :: ")
  //      words.foreach(println)
  //      println("|||||||||||||||||||||||||||||||||||||||||||||||||||||")
  //    }
  //})




}