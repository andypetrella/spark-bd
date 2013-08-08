spark-bd
========

Exploration of spark streaming based on the BigData.be project 2

To configure twitter, two options:
* edit application.conf and set oauth2 required information
* export/set system environment variables:
  * TWITTER_OAUTH_CONS_KEY = consumer key
  * TWITTER_OAUTH_CONS_SEC = consumer secret
  * TWITTER_OAUTH_ACC_TKN = access token
  * TWITTER_OAUTH_TKN_SEC = access token secret

To run it
> sbt
> run-main be.bigdata.p2.P2 both GOOG AAPL ORCL YHOO CSCO INTL AMD IBM HPQ MSFT

For now, it will simply compute a score every 60" based on streamed information it got from twitter and the yahoo finance csv (that it requests as often as possible).

Those results are, for now, stored in text file on the root of the project... It's simply a flush of all RDD (every 5") rather than a real flush every 60" (even if the score is based on this window).