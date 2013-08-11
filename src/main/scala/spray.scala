package spark


import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.util.Timeout
import akka.util.duration._
import akka.pattern.ask

import cc.spray._
import cc.spray.json._
import cc.spray.typeconversion.SprayJsonSupport

import spark.util.AkkaUtils
import spark.streaming.DStream

import be.bigdata.p2._

object SparkSpray extends Directives {
  case class Result(stock:Stock, score:Int)

  object MyJsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {
    implicit object StockJsonFormat extends RootJsonFormat[Stock] {
      def write(s: Stock) =
        JsObject(
          "id" -> JsString(s.id),
          "keywords" -> s.keywords.toJson
        )

      def read(value: JsValue) =
        value.asJsObject.getFields("id", "keywords") match {
          case Seq(JsString(id), JsArray(keywords)) =>
            Stock(id, keywords.collect { case JsString(s) => s })
          case _ => throw new DeserializationException("Stock expected")
        }
    }
    implicit val ResultFormat = jsonFormat2(Result)
  }

  class ResultActor() extends Actor {
    var cache:Map[String, List[Result]] = Map.empty

    def receive = {
      case d@(stock:Stock, i:Int) => cache = cache + (stock.id -> (Result(stock, i) :: cache.get(stock.id).getOrElse(Nil)))
      case "results" => sender ! cache
      //case x => println(("*"*100)+"ERROR : " + x)
    }
  }

  lazy val actorSystem = SparkAkka.actorSystem

  def start() = {
    val actor = actorSystem.actorOf(Props(new ResultActor()), name = "results")

    (
      AkkaUtils.startSprayServer(SparkAkka.actorSystem, SparkAkka.host, SparkAkka.port+1, handler(actor), "Results")
      ,
      actor
    )
  }

  private[this] implicit val timeout = Timeout(10 seconds)
  import MyJsonProtocol._
  private[this] def handler(actor:ActorRef) = {
    get {
      path("") {
        completeWith {
          "use /results"
        }
      } ~
      path("results") {
        completeWith {
          (actor ? "results").mapTo[Map[String, List[Result]]]
        }
      }
    }
  }
}