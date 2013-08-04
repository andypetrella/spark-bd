package spark

import spark.util.AkkaUtils

class SparkAkka {

  lazy val actorSystem = AkkaUtils.createActorSystem("sparkAkka", "localhost", 10123)._1


}