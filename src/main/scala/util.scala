package be.bigdata.p2

object Implicits {
    implicit def entryToTuple[A,B](e:java.util.Map.Entry[A,B]):(A,B) = (e.getKey, e.getValue)
}