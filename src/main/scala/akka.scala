package spark

import spark.util.AkkaUtils

object SparkAkka {

  private[this] lazy val config = be.bigdata.p2.conf.root.getConfig("deploy.akka")

  lazy val name = config.getString("name")
  lazy val host = config.getString("host")
  lazy val port = config.getInt("port")

  lazy val actorSystem = AkkaUtils.createActorSystem(name, host, port)._1

  def urlFor(actorName:String) = "akka://"+name+"@"+host+":"+port+"/user/"+actorName
}