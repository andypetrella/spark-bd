package be.bigdata.p2

import com.typesafe.config._

import spark.streaming.{Seconds, StreamingContext}
import spark.streaming.StreamingContext._
import spark.SparkContext._

import scala.collection.JavaConversions._

object P2 extends App {
  //load the conf
  val conf = ConfigFactory.load(getClass.getClassLoader);

  //prepare auth to twitter
  val twitterAuth = conf.getConfig("twitter.oauth")
  twitterAuth
    .entrySet()
    .map(x => (x.getKey(), x.getValue()))
    .foreach{
      case (p,v) => System.setProperty("twitter4j.oauth."+p, v.unwrapped.toString)
    }

  //compute sentiments
  val blankChar = " "(0)
  lazy val sentiments = scala.io.Source.fromURL(getClass.getResource("/sentiments.txt"), "utf-8").getLines.map { l =>
    val (rs, rw) = l.reverse.span(_ != blankChar)
    (rw.reverse.trim.toLowerCase, rs.reverse.toInt)
  }.toMap


  //init spark
  val ssc = new StreamingContext("local", "Project2", Seconds(5))

  val topics = args.toSeq

  //create twitter stream using filter from the args list
  val stream = ssc.twitterStream(None, topics)

  //map tweets to their sentiment score
  val statusWithSentiment = stream.map{ status =>
    val text = status.getText.toLowerCase
    val (score, words) = sentiments.foldLeft((0, List():List[String])) { case ((score, ss), (w, s)) =>
      if (text == w
          || text.startsWith(w + " ")
          || text.endsWith(" " + w)
          || text.contains(" " + w + " ")) {
        (score  + s, w :: ss)
      } else {
        (score, ss)
      }
    }
    (score, words, status)
  }

  val topSentimental60sec =
    statusWithSentiment
      .map{ case (x, y, z) =>
        (x, List((z,y)))
      }


  val score60secGroupedByTopic =
    topSentimental60sec
      .filter { case (score, (status, words) :: Nil) => !words.isEmpty}
      .flatMap { case (score, (status, words) :: Nil) =>
        //produce one element by topic included in the status
        // in the case where the status matches several topics...
        topics
          .filter(topic => status.getText.contains("#"+topic))
          .map(topic => (topic, List((status, words, score))))
      }
      .reduceByKeyAndWindow(_ ::: _, Seconds(60))
      //for each topic => accScore, participatingTweets, allParticipatingSentiments
      .mapValues(xs => (xs.map(_._3).sum, xs.map(_._1), xs.flatMap(_._2)))



  val orderedTopSentimental60sec = topSentimental60sec
                                    .reduceByKeyAndWindow(_ ::: _, Seconds(60))
                                    .transform(_.sortByKey(false))

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
  score60secGroupedByTopic
    .saveAsTextFiles("scoreByTopic", "60sec")

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



  ssc.start()

}