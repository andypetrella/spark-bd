package be.bigdata.p2

import com.typesafe.config._

import spark.streaming.{Seconds, StreamingContext}
import spark.streaming.StreamingContext._
import spark.SparkContext._

import scala.collection.JavaConversions._

class Twitter(twitterConfig:Config) {

  twitterConfig
    .entrySet()
    .map(x => (x.getKey(), x.getValue()))
    .foreach{
      case (p,v) => System.setProperty("twitter4j.oauth."+p, v.unwrapped.toString)
    }


  def apply(stocks:Seq[String])(implicit ssc:StreamingContext) = {
    //create twitter stream using filter from the args list
    val stream = ssc.twitterStream(None, stocks)


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
          st.getHashtagEntities.map{_.getText}.collect {
            case stock if stocks.contains(stock) => (stock, (ss, st))
          }
        }

    sentimentsAndStatusByStock
  }


}