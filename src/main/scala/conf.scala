package be.bigdata.p2

import com.typesafe.config._

object conf {
    //load the conf
  lazy val root = ConfigFactory.load(getClass.getClassLoader);

}