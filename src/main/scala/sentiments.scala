package be.bigdata.p2

import scala.util.matching.Regex

case class Sentiment(word:String, score:Int)

object sentiments {

  private[this] val blankChar = " "(0)
  //compute sentiments
  lazy val map = scala.io.Source.fromURL(getClass.getResource("/sentiments.txt"), "utf-8")
                                  .getLines.map { l =>
                                    //fetch the last part which is the score...
                                    val (rs, rw) = l.reverse.span(_ != blankChar)
                                    val s = Sentiment(rw.reverse.trim.toLowerCase, rs.reverse.toInt)
                                    (s.word, s)
                                  }
                                  .toMap

  def sentMatcher(w:String) =
    String.format(
      "^(%s)$|^(%s)\\s+.*|.*\\s+(%s)$|.*\\s+(%s)\\s+.*",
      Seq.fill(4)(w):_*
    ).r

  def matchIt(r:Regex) = (t:String) =>
    t match {
      case s@r(_*)             => Some(s)
      case _                    => None
    }

  lazy val sentimentMatchers = map.map { case (w, sent) =>
    (matchIt(sentMatcher(w)), sent)
  }

  def compute(text:String) = {
    val t = text.toLowerCase
    sentimentMatchers.foldLeft((Nil:List[Sentiment])) {
      case (ss, (m, s)) =>
        m(t) match {
          case Some(d) => s :: ss
          case None    => ss
        }
    }
  }
}