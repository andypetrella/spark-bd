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

These computation are sent to an actor that holds the timeline of each stock. Note: the timeline is composed of the windowed computation of 60" based period, and the RDD are 5" length; 
so there is an overlap between windows. That says that the new scores aren't added to the previous one, but each window constitute an estimation of the variation in the mood.

A spray server is started on the address [http://localhost:10124](http://localhost:10124), the "/results" route is dedicated to fetching all timelines kept by the above actor.

So it's possible to hit [http://localhost:10124/results](http://localhost:10124/results) and get the results in the following unoptimized form:

```
{
    "MSFT": [
        {
            "stock": {
                "id": "MSFT",
                "keywords": [
                    "Microsoft",
                    "Windows"
                ]
            },
            "score": 1
        },
        {
            "stock": {
                "id": "MSFT",
                "keywords": [
                    "Microsoft",
                    "Windows"
                ]
            },
            "score": 1
        }
    ],
    "YHOO": [
        {
            "stock": {
                "id": "YHOO",
                "keywords": [
                    "yahoo"
                ]
            },
            "score": 2
        },
        {
            "stock": {
                "id": "YHOO",
                "keywords": [
                    "yahoo"
                ]
            },
            "score": 1
        }
    ],
    "AAPL": [
        {
            "stock": {
                "id": "AAPL",
                "keywords": [
                    "apple",
                    "ios",
                    "iphone",
                    "ipad"
                ]
            },
            "score": 8
        },
        {
            "stock": {
                "id": "AAPL",
                "keywords": [
                    "apple",
                    "ios",
                    "iphone",
                    "ipad"
                ]
            },
            "score": 7
        }
    ],
    "AMD": [
        {
            "stock": {
                "id": "AMD",
                "keywords": [
                    
                ]
            },
            "score": -1
        },
        {
            "stock": {
                "id": "AMD",
                "keywords": [
                    
                ]
            },
            "score": -1
        }
    ],
    "HPQ": [
        {
            "stock": {
                "id": "HPQ",
                "keywords": [
                    
                ]
            },
            "score": -1
        },
        {
            "stock": {
                "id": "HPQ",
                "keywords": [
                    
                ]
            },
            "score": -1
        }
    ],
    "ORCL": [
        {
            "stock": {
                "id": "ORCL",
                "keywords": [
                    "oracle",
                    "java",
                    "mysql"
                ]
            },
            "score": -1
        },
        {
            "stock": {
                "id": "ORCL",
                "keywords": [
                    "oracle",
                    "java",
                    "mysql"
                ]
            },
            "score": -1
        }
    ],
    "INTL": [
        {
            "stock": {
                "id": "INTL",
                "keywords": [
                    "intel"
                ]
            },
            "score": 2
        },
        {
            "stock": {
                "id": "INTL",
                "keywords": [
                    "intel"
                ]
            },
            "score": 2
        }
    ],
    "GOOG": [
        {
            "stock": {
                "id": "GOOG",
                "keywords": [
                    "google",
                    "android",
                    "chrome"
                ]
            },
            "score": 15
        },
        {
            "stock": {
                "id": "GOOG",
                "keywords": [
                    "google",
                    "android",
                    "chrome"
                ]
            },
            "score": 12
        }
    ],
    "IBM": [
        {
            "stock": {
                "id": "IBM",
                "keywords": [
                    
                ]
            },
            "score": -1
        },
        {
            "stock": {
                "id": "IBM",
                "keywords": [
                    
                ]
            },
            "score": -1
        }
    ],
    "CSCO": [
        {
            "stock": {
                "id": "CSCO",
                "keywords": [
                    "cisco"
                ]
            },
            "score": -1
        },
        {
            "stock": {
                "id": "CSCO",
                "keywords": [
                    "cisco"
                ]
            },
            "score": -1
        }
    ]
}
```
