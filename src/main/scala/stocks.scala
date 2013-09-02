package be.bigdata.p2


case class Stock(
  id:String,
  keywords:List[String] = Nil,
  name:String,
  year:Option[Int],
  sector:String,
  industry:String
) {
  @transient lazy val all = id :: name :: keywords
}

object Stocks {

  lazy private[this] val raw = scala.io.Source.fromURL(getClass.getResource("/companyList.csv"), "utf-8")
                                  .getLines
                                  .drop(1)
                                  .map { l =>
                                    l.substring(1, l.length-2).split("\",\"").toList match {
                                      case List(symbol, name, "n/a", sector, industry) =>
                                        Stock(symbol, Nil, name, None, sector, industry)
                                      case List(symbol, name, year, sector, industry) =>
                                        Stock(symbol, Nil, name, Some(year.toInt), sector, industry)
                                    }
                                  }
                                  .toList


  lazy val hard = raw.map {
    case s if s.id == "GOOG" => s.copy(keywords = List("google", "android", "chrome"))
    case s if s.id == "AAPL" => s.copy(keywords = List("apple", "ios", "iphone", "ipad"))
    case s if s.id == "ORCL" => s.copy(keywords = List("oracle", "java", "mysql"))
    case s if s.id == "YHOO" => s.copy(keywords = List("yahoo"))
    case s if s.id == "CSCO" => s.copy(keywords = List("cisco"))
    case s if s.id == "INTL" => s.copy(keywords = List("intel"))
    case s if s.id == "MSFT" => s.copy(keywords = List("Microsoft", "Windows"))
    case s                   => s
  }

  private[this] val defaults = hard.map(x => (x.id, x)).toMap

  def get(s:String) = defaults.get(s).getOrElse(Stock(s, Nil, s, None, "", ""))
}
