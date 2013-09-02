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
  case class Result(stock:Stock, score:Int, time:Long)

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
            val s = Stocks.get(id)
            s.copy(keywords = s.keywords ++ keywords.collect { case JsString(s) => s })
          case _ => throw new DeserializationException("Stock expected")
        }
    }
    implicit val ResultFormat = jsonFormat3(Result)
  }

  class ResultActor() extends Actor {
    var cache:Map[String, List[Result]] = Map.empty

    def receive = {
      case d@(stock:Stock, i:Int, time:Long) => cache = cache + (stock.id -> (Result(stock, i, time) :: cache.get(stock.id).getOrElse(Nil)))
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

  def safeMax(s:Seq[Long]) = if (s.isEmpty) 0 else s.max

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
      } ~
      path("after") {
        parameters('time ?).as((x:Option[Long]) => x) { t =>
          completeWith {
            val time = t.getOrElse(0L)
            (actor ? "results")
              .mapTo[Map[String, List[Result]]]
              .map { results =>
                results.map { case (s, ls) =>
                  (s, ls.takeWhile(_.time > time))
                }
              }.map { results =>
                (
                  results,
                  safeMax(
                    results
                      .view
                        .map(_._2)
                        .flatten
                        .map(_.time)
                        .toSeq
                  )
                )
              }
          }
        }
      } ~
      pathPrefix("web") {
        cache {
          getFromResourceDirectory("web")
        }
      } ~
      path("start") {
        completeWith {
          P2.start
          "started"
        }
      } ~
      path("stop") {
        completeWith {
          P2.stop
          "stopped"
        }
      }
    }
  }
}