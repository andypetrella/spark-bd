package be.bigdata.p2

import scala.util.matching.Regex
import akka.util.duration._

import com.typesafe.config._

import akka.actor.{Actor, ActorRef, Props, PoisonPill}

import spark.streaming.{Seconds, StreamingContext}
import spark.streaming.StreamingContext._
import spark.streaming.receivers.Receiver
import spark.SparkContext._

import java.net.URL


object Tick
case class For(actor:ActorRef, stocks:Seq[Stock])
case class Consume(actor:ActorRef, url:URL)

case class YahooData(
  stock:Stock,
  trade:Double,
  date:String,
  time:String,
  delta:(Double, Double),
  volume:Int) extends Data
object YahooData {
  def na(s:String, pre:String=>String=identity):Double = if (s == "N/A") Double.MinValue else pre(s).toDouble

  def create(a:Map[String, String]) = {
    a.get("e1")
      .flatMap(x => if (x=="N/A") Some(x) else None)
      .map {_ =>
       YahooData(
        stock = Stocks.get(a("s")),
        trade = na(a("l1")),
        date = a("d1"),
        time = a("t1"),
        delta = (na(a("c6")), na(a("p2"), _.init.mkString)),
        volume = na(a("v")).toInt
       )
      }
  }
}

class Yahoo(feeder:String) extends Serializable {

  def apply(stocks:Seq[Stock])(implicit @transient ssc:StreamingContext) = {
    ssc.actorStream[YahooData](Props(new YahooActorReceiver(feeder, stocks)), "YahooReceiver")
  }
}

class YahooActorReceiver(feeder:String, stocks:Seq[Stock]) extends Actor with Receiver {

  // cache here the last change for each stock
  // then not push the block if it didn't changed...
  var lasts:Map[String, YahooData] = Map.empty

  lazy private val remotePublisher = context.actorFor(feeder)

  override def preStart = remotePublisher ! For(context.self, stocks)

  def receive = {
    case y:YahooData ⇒ {
      val push = lasts
                  .get(y.stock.id)
                  .map(_ != y)
                  .getOrElse(true)

      lasts = lasts + (y.stock.id -> y)

      if (push) {
        pushBlock(y)
      }
    }
  }

  override def postStop() = () //remotePublisher ! UnsubscribeReceiver(context.self)
}

class FeederActor extends Actor {
  // http://cliffngan.net/a/13
  val yahooResponseFormat = List("e1", "s", "l1", "d1", "t1", "c6", "p2", "v")
  val yahooService        = "http://finance.yahoo.com/d/quotes.csv?f=%s&e=.csv&s=";
  val yahooUrlTmpl        = String.format(yahooService, yahooResponseFormat.mkString)
  //def financeData(stocks:Seq[String]) = String.format(yahooService, stocks.mkString(","), yahooResponseFormat.mkString)


  var urls:Option[Seq[URL]] = None

  var ref:Option[ActorRef] = None

  private[this] def urlFriendlySeq(s:Seq[String], append:(String, String)=>String)(base:String, size:Int=1024):Seq[String] = {
    s.foldLeft(Nil:List[String]) {
      case (Nil, s) => List(append(base, s)) //case where s too long not handled :-/
      case (l, s) =>
        val x = l.head
        val xs = l.tail
        append(x, s) match {
          case a if a.length > size => append(base, s) :: l //case where s too long not handled :-/
          case a                    => a :: xs
        }
    }
    .toSeq
  }

  private[this] def splitYUrls(stocks:List[String]):Seq[String] = {
    assert(yahooUrlTmpl.endsWith("="))
    urlFriendlySeq(stocks, (b:String, s:String) => if (b.endsWith("=")) b+s else b+"+"+s)(yahooUrlTmpl)
  }


  def receive = {

    case For(sparkled, stocks)  =>
      ref = Some(sparkled)
      urls = Some(splitYUrls(stocks.map(_.id).toList).map(x => new URL(x)))

    case Tick           =>
      for {
        actor <- ref
        us    <- urls
      } {
        val n = System.nanoTime
        us.zipWithIndex.foreach { case (u, i) =>
          val a = context.actorOf(Props[FeederActor], "FeederActor-"+n+"-"+i)
          a ! Consume(actor, u)
        }
      }

    case Consume(actor, url) => {
      import java.io.{BufferedReader, InputStreamReader}
      val b = new BufferedReader(new InputStreamReader(url.openStream, "utf-8"))
      Stream
        .continually(b.readLine)
        .takeWhile(_ != null)
        .map { l =>
          YahooData.create((yahooResponseFormat zip l.replace("\"","").split(",")).toMap)
        }
        .foreach {
          case None    => ()
          case Some(y) => actor ! y
        }
      self ! PoisonPill
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