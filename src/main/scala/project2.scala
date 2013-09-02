package be.bigdata.p2

import scala.collection.JavaConversions._

import akka.actor.{Actor, ActorRef, ActorSystem, Props}

import spark.streaming.{Seconds, StreamingContext, DStream}
import spark.streaming.StreamingContext._
import spark.SparkContext._

trait Data {
  def stock: Stock
}

object P2 extends App {
  //start the spray server
  val (server, actor) = spark.SparkSpray.start()

  // keep this alive
  import akka.util.duration._
  val keepAlive = spark.SparkAkka.actorSystem.scheduler.schedule(0 second, 1 second)(println("Still alive and well (Johnny Winter)"))

  //prepare auth to twitter
  val twitterAuth = conf.root.getConfig("twitter.oauth")

  val deploy = args(0)

  //init spark
  implicit val ssc = deploy match {
    case "local" =>
      new StreamingContext("local", "Project2", Seconds(5))
    case x       =>
      val c = x.split(";")
      // spark://noootsab:7077
      // /home/noootsab/src/github/spark
      // p2.jar;...
      new StreamingContext(c(0), "Project2", Seconds(5), c(1), c.drop(2).toList)
  }

  val action = args(1)
  val printing = args(2) == "print"
//  val stocks = args.drop(if (printing) 3 else 2).toSeq.map(Stocks.get)

  lazy val twitter = new Twitter(twitterAuth)
  lazy val twitterDStream:DStream[Data] = twitter(Stocks.hard).asInstanceOf[DStream[Data]]

  lazy val yahoo = new Yahoo(spark.SparkAkka.urlFor("FeederActor"))
  lazy val yahooDStream:DStream[Data] = yahoo(Stocks.hard).asInstanceOf[DStream[Data]]


  lazy val start = {
    if (action == "yahoo" || action == "both") {
      Yahoo.start
      if (printing) yahooDStream.foreach { rdd => rdd.foreach { x => println(x) } }
    }
    if (action == "twitter" || action == "both") {
      if (printing) twitterDStream.foreach { rdd => rdd.foreach { x => println(x) } }
    }

    if (action == "both") {
      val both = twitterDStream union yahooDStream

      val asString:Data => String = (_:Data) match {
        case d:YahooData => "Yahoo at " + d.time + " change : " + d.delta
        case d:TwitterData => "Tweet by " + d.status.getUser.getName + " : " + d.status.getText
      }

      val score = (_:Data) match {
        case x:YahooData => if (x.delta._2 < 0) -1 else 1
        case x:TwitterData => if (x.sentiments.map(_.score).sum < 0) -1 else 1
      }

      val computed = both
        .map(x => (x.stock, List(x)))
        .reduceByKeyAndWindow(_ ::: _, Seconds(60))
        .mapValues(xs => (xs.map(score).sum, xs.foldLeft((0,0)) {
          case ((y,t), x:YahooData) => (y+1,t)
          case ((y,t), x:TwitterData) => (y,t+1)
        }))

      computed.foreach { (rdd, time) =>
        rdd.foreach {
          case (stock, (score, (y,t))) =>
            //FIXME :: Re-fetching the `actor` in the DStream function...
            //... quick fix to avoid its serialization problem
            spark.SparkAkka.actorSystem.actorFor(
              spark.SparkAkka.urlFor("results")
            ) ! (stock, score, time.milliseconds)
        }
      }
    }
    ssc.start()
  }

  lazy val stop = {
    ssc.stop()
    keepAlive.cancel()
    spark.SparkAkka.actorSystem.shutdown()
  }

  while (!keepAlive.isCancelled) {
    println("Sleeping 1 second")
    Thread.sleep(1000)
  }

}