package be.bigdata.p2

import com.typesafe.config._

import spark.streaming.{Seconds, StreamingContext}
import spark.streaming.StreamingContext._
import spark.SparkContext._

import twitter4j._

import scala.collection.JavaConversions._

case class TwitterData(
  stock:Stock,
  sentiments:Seq[Sentiment],
  status:Status) extends Data

class Twitter(twitterConfig:Config) {

  twitterConfig
    .entrySet()
    .map(x => (x.getKey(), x.getValue()))
    .foreach{
      case (p,v) => System.setProperty("twitter4j.oauth."+p, v.unwrapped.toString)
    }

  def apply(stocks:Seq[Stock])(implicit ssc:StreamingContext) = {
    //create twitter stream using filter from the args list
    val stream = ssc.twitterStream(None)//, stocks.flatMap(_.all))

    //map tweets to their sentiments, then collapse all non relevant tweets (no matching sentiment)
    val statusWithNotEmptySentiments =
      stream
        .map{ status =>
          (sentiments.compute(status.getText), status)
        }
        .filter {
          case (ss, st) => !ss.isEmpty
        }

    val sentimentsAndStatusByStock =
      statusWithNotEmptySentiments
        .flatMap { case (ss, st) =>
          val ts = (st.getText + " " + st.getUser.getName)
                    .toLowerCase
                    .replaceAll("[#@\\$]", " ")
                    .split("\\s+")
                    .toSet

          stocks.collect {
            case s if s.all.exists(x => ts(x.toLowerCase)) => TwitterData(s, ss, st)
          }
        }

    sentimentsAndStatusByStock
  }
}