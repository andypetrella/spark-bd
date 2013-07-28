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

  //create twitter stream using filter from the args list
  val stream = ssc.twitterStream(None, args.toSeq)

  //map tweets to their sentiment score
  val statusWithSentiment = stream.map{ status =>
    val text = status.getText.toLowerCase
    val scoreNWords = sentiments.foldLeft((0, List():List[String])) { case ((score, ss), (w, s)) =>
      if (text == w
          || text.startsWith(w + " ")
          || text.endsWith(" " + w)
          || text.contains(" " + w + " ")) {
        (score  + s, w :: ss)
      } else {
        (score, ss)
      }
    }
    (scoreNWords._1, scoreNWords._2, status)
  }

  val topSentimental60sec =
    statusWithSentiment
      .map{ case (x, y, z) =>
        (x, List((z,y)))
      }
      .reduceByKeyAndWindow(_ ::: _, Seconds(60))
      //.map{case (score, tweets) => (tweets, score)}
      .transform(_.sortByKey(false))

  // Print popular hashtags
  topSentimental60sec.foreach(rdd => {
    val topList = rdd.take(5)
    println("\nMost sentimental tweets in last 60 seconds (%s total):".format(rdd.count()))
    topList
      .foreach{case (sentiment, statuses) =>
        println("SENTIMENT :: " + sentiment)
        println("TWEETS :: ")
        statuses.map(x => "   " + x._1.getText + " |>>| " + x._2.mkString("|@|")).foreach(println)
        println("|||||||||||||||||||||||||||||||||||||||||||||||||||||")
      }
  })

  ssc.start()

}