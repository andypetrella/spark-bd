package be.bigdata.p2

import scala.util.matching.Regex
import akka.util.duration._

import com.typesafe.config._

import akka.actor.{Actor, ActorRef, Props}

import spark.streaming.{Seconds, StreamingContext}
import spark.streaming.StreamingContext._
import spark.streaming.receivers.Receiver
import spark.SparkContext._

class Yahoo(feeder:String) extends Serializable {

  def apply(stocks:Seq[String])(implicit @transient ssc:StreamingContext) = {
    ssc.actorStream[YahooStock](Props(new YahooActorReceiver(feeder, stocks)), "YahooReceiver")
  }

}

class YahooActorReceiver(feeder:String, stocks:Seq[String]) extends Actor with Receiver {

  // cache here the last change for each stock
  // then not push the block if it didn't changed...
  var lasts:Map[String, YahooStock] = Map.empty

  lazy private val remotePublisher = context.actorFor(feeder)

  override def preStart = remotePublisher ! For(context.self, stocks)

  def receive = {
    case msg ⇒ {
      val y = msg.asInstanceOf[YahooStock]

      val push = lasts
                  .get(y.track)
                  .map(_ != y)
                  .getOrElse(true)

      lasts = lasts + (y.track -> y)

      if (push) {
        context.parent ! pushBlock(y)
      }
    }
  }

  override def postStop() = () //remotePublisher ! UnsubscribeReceiver(context.self)

}

case class YahooStock(
  track:String,
  trade:Double,
  date:String,
  time:String,
  delta:(Double, Double),
  volume:Int)
object YahooStock {
  def na(s:String, pre:String=>String=identity):Double = if (s == "N/A") Double.MinValue else pre(s).toDouble

  def create(a:Map[String, String]) = {
    a.get("e1")
      .flatMap(x => if (x=="N/A") Some(x) else None)
      .map {_ =>
       YahooStock(
        track = a("s"),
        trade = na(a("l1")),
        date = a("d1"),
        time = a("t1"),
        delta = (na(a("c6")), na(a("p2"), _.init.mkString)),
        volume = na(a("v")).toInt
       )
      }
  }
}

class FeederActor extends Actor {
  import java.net.URL

  // http://cliffngan.net/a/13
  val yahooResponseFormat = List("e1", "s", "l1", "d1", "t1", "c6", "p2", "v")
  val yahooService        = "http://finance.yahoo.com/d/quotes.csv?s=%s&f=%s&e=.csv";

  def financeData(stocks:Seq[String]) = String.format(yahooService, stocks.mkString(","), yahooResponseFormat.mkString)

  var url:Option[URL] = None

  var ref:Option[ActorRef] = None

  def receive = {

    case For(sparkled, stocks)  =>
      ref = Some(sparkled)
      url = Some(new URL(financeData(stocks)))

    case Tick           =>
      ref.foreach { actor =>
        import java.io._
        url.foreach { _u =>
          val b = new BufferedReader(new InputStreamReader(_u.openStream, "utf-8"))
          Stream.continually(b.readLine).takeWhile(_ != null).foreach { l =>
            YahooStock
              .create(
                (yahooResponseFormat zip l.replace("\"","").split(",")).toMap
              ).foreach { y =>
                actor ! y
              }
          }
        }
      }
  }
}
object Yahoo {
  import spark.SparkAkka._

  lazy val feeder = actorSystem.actorOf(Props[FeederActor], "FeederActor")

  def start = {
    actorSystem.scheduler.schedule(0 milliseconds, 500 milliseconds, feeder, Tick)
  }
}