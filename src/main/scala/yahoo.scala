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
    ssc.actorStream[String](Props(new YahooActorReceiver[YahooStock](feeder, stocks)), "YahooReceiver")
  }

}

class YahooActorReceiver[T: ClassManifest](feeder:String, stocks:Seq[String]) extends Actor with Receiver {

  //TODO maybe cache here the last trade time (t1) for each stock
  // then not push the block if it didn't changed...

  lazy private val remotePublisher = context.actorFor(feeder)

  override def preStart = remotePublisher ! For(context.self, stocks)

  def receive = {
    case msg ⇒ context.parent ! pushBlock(msg.asInstanceOf[T])
  }

  override def postStop() = () //remotePublisher ! UnsubscribeReceiver(context.self)

}

case class YahooStock(
  track:String,
  quote:Double,
  date:String,
  time:String,
  delta:(Double, Double),
  number:Int)
object YahooStock {
  def clean(s:String) = s.replace("\"","")

  def na(s:String):Double = if (s == "N/A") Double.MinValue else s.toDouble

  def apply(a:Array[String]) = {
    val _a = a.map(clean _)
    new YahooStock(
      _a(0).replace("\"",""),
      na(_a(1)),
      _a(2),
      _a(3),
      _a(4)
        .split(" - ")
        .toList match {
          case "N/A"::"N/A"::Nil => (Double.MinValue, Double.MinValue)
          case a::b::Nil => (na(a),na(b.init.mkString))
          case _ => (Double.MinValue, Double.MinValue)
        },
      na(_a(5)).toInt
    )
  }
}

class FeederActor extends Actor {
  import java.net.URL

  // http://cliffngan.net/a/13
  // TODO probably:
  //     add "e1"
  //     use "c6" and "k2" rather than "c"

  val yahooResponseFormat = "sl1d1t1cv";
  val yahooService        = "http://finance.yahoo.com/d/quotes.csv?s=%s&f=%s&e=.csv";

  def financeData(stocks:Seq[String]) = String.format(yahooService, stocks.mkString(","), yahooResponseFormat)

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
            actor ! YahooStock(l.split(","))
          }
        }
      }
  }
}
object Yahoo {
  lazy val actorSystem =  new spark.SparkAkka().actorSystem
  lazy val feeder = actorSystem.actorOf(Props[FeederActor], "FeederActor")

  def start = {
    actorSystem.scheduler.schedule(0 milliseconds, 500 milliseconds, feeder, Tick)
  }
}