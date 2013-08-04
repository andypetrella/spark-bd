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

  // http://cliffngan.net/a/13
  val yahooResponseFormat = "sl1d1t1cv";
  val yahooService        = "http://finance.yahoo.com/d/quotes.csv?s=%s&f=%s&e=.csv";

  def financeData(stocks:Seq[String]) = String.format(yahooService, stocks.mkString(","), yahooResponseFormat)

  def apply(stocks:Seq[String])(implicit @transient ssc:StreamingContext) = {
    ssc.actorStream[String](Props(new YahooActorReceiver[String](feeder)), "YahooReceiver")
  }

}

class YahooActorReceiver[T: ClassManifest](feeder:String) extends Actor with Receiver {

  lazy private val remotePublisher = context.actorFor(feeder)

  override def preStart = remotePublisher ! For(context.self)

  def receive = {
    case msg ⇒ msg.asInstanceOf[Seq[T]].foreach(t => context.parent ! pushBlock(t))
  }

  override def postStop() = () //remotePublisher ! UnsubscribeReceiver(context.self)

}

class FeederActor extends Actor {
  var ref:Option[ActorRef] = None
  def receive = {
    case For(sparkled)  => ref = Some(sparkled)
    case Tick           => ref.foreach { actor =>
      actor ! Seq.fill((scala.util.Random.nextDouble*1000).toInt)("csv, lines, from, yahoo")
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