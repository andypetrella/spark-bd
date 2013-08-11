package be.bigdata.p2

import scala.collection.JavaConversions._

import akka.actor.{Actor, ActorRef, ActorSystem, Props}

import spark.streaming.{Seconds, StreamingContext, DStream}
import spark.streaming.StreamingContext._
import spark.SparkContext._

trait Data {
  def stock: Stock
}

case class Stock(
  id:String,
  keywords:List[String]
) {
  @transient lazy val all = id :: keywords
}

object Stocks {
  private[this] val seq = Seq(
    Stock("GOOG", List("google", "android", "chrome")),
    Stock("AAPL", List("apple", "ios", "iphone", "ipad")),
    Stock("ORCL", List("oracle", "java", "mysql")),
    Stock("YHOO", List("yahoo")),
    Stock("CSCO", List("cisco")),
    Stock("INTL", List("intel")),
    Stock("AMD", Nil),
    Stock("IBM", Nil),
    Stock("MSFT", List("Microsoft", "Windows"))
  )

  private[this] val defaults = seq.map(x => (x.id, x)).toMap

  def get(s:String) = defaults.get(s).getOrElse(Stock(s, Nil))
}

object Tick
case class For(actor:ActorRef, stocks:Seq[Stock])

object P2 extends App {
  //prepare auth to twitter
  val twitterAuth = conf.root.getConfig("twitter.oauth")

  //init spark
  implicit val ssc = new StreamingContext("local", "Project2", Seconds(5))

  val action = args(0)
  val printing = args(1) == "print"
  val stocks = args.drop(if (printing) 2 else 1).toSeq.map(Stocks.get)

  lazy val twitter = new Twitter(twitterAuth)
  lazy val twitterDStream:DStream[Data] = twitter(stocks).asInstanceOf[DStream[Data]]

  lazy val yahoo = new Yahoo(spark.SparkAkka.urlFor("FeederActor"))
  lazy val yahooDStream:DStream[Data] = yahoo(stocks).asInstanceOf[DStream[Data]]

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
      //.mapValues(xs => (xs.map(score).sum, xs.map(asString)))
      .mapValues(xs => (xs.map(score).sum, xs.foldLeft((0,0)) {
        case ((y,t), x:YahooData) => (y+1,t)
        case ((y,t), x:TwitterData) => (y,t+2)
      }))
      //.saveAsTextFiles("scoreByStock", "last60sec")

    val (server, actor) = spark.SparkSpray.start()
    computed.foreach { rdd =>
      rdd.foreach {
        case (stock, (score, (y,t))) =>
          //FIXME :: Re-fetching the `actor` in the DStream function...
          //... quick fix to avoid its serialization problem
          spark.SparkAkka.actorSystem.actorFor(
            spark.SparkAkka.urlFor("results")
          ) ! (stock, score)
      }
    }
  }

  ssc.start()
}