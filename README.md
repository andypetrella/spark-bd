spark-bd
========

Exploration of spark streaming based on the BigData.be project 2.

I also tried to relate some of the work done here in my blog. See [this page](http://ska-la.blogspot.be/2013/08/mid-2013-my-spark-odyssey.html) for the very first post.


Configure
---------

To configure twitter, two options:
* edit application.conf and set oauth2 required information
* export/set system environment variables:
  * TWITTER_OAUTH_CONS_KEY = consumer key
  * TWITTER_OAUTH_CONS_SEC = consumer secret
  * TWITTER_OAUTH_ACC_TKN = access token
  * TWITTER_OAUTH_TKN_SEC = access token secret

Launch
------

To run it (see below for a version using the embedded webapp)

> sbt

> run-main be.bigdata.p2.P2 both GOOG AAPL ORCL YHOO CSCO INTL AMD IBM HPQ MSFT

> curl http://localhost:10124/start

to stop it

> curl http://localhost:10124/stop



For now, it will simply compute a score every 60" based on streamed information it got from twitter and the yahoo finance csv (that it requests as often as possible).

These computation are sent to an actor that holds the timeline of each stock. Note: the timeline is composed of the windowed computation of 60" based period, and the RDD are 5" length; 
so there is an overlap between windows. That says that the new scores aren't added to the previous one, but each window constitute an estimation of the variation in the mood.

Embedded webapp
---------------

A spray server is started on the address [http://localhost:10124](http://localhost:10124), the "/results" route is dedicated to fetching all timelines kept by the above actor.

So it's possible to hit [http://localhost:10124/results](http://localhost:10124/results) and get the results in an unoptimized form (see below).

Also, there is a built-in interface that can show the results in real-time when the spark streaming has been started. 
This UI is available at this URL by default: [http://localhost:10124/web/index.html](http://localhost:10124/web/index.html). This will look like this:
![UI Preview](https://raw.github.com/andypetrella/spark-bd/master/preview-ui.png "UI Preview")

Since this project a pure test-the-technology one, I've added a delayed startup of the Spark stuffs (helpful when debugging the interface only ^^).
So the start time is delayed until the `start` value on the `P2` object is accessed, also there is a `stop` one that will stop the whole thing including the Akka actor system.

However, these calls are enabled through the UI (two buttons in the navbar).

To recap: 
 1. `run-main be.bigdata.p2.P2 both GOOG AAPL ORCL YHOO CSCO INTL AMD IBM HPQ MSFT`
 2. go to [http://localhost:10124/web/index.html](http://localhost:10124/web/index.html)
 3. click on **start**
 4. see the analysis performing in realtime in the graph
 5. click on **stop**
 6. go to 1/ to restart everything

Actually, the realtime graph is constructed using another end-point in the Spray route `/after?time={arg}`. Calling this route will retrieve and serialize all events since the specified time parameter,
zero if none. Additionaly, it'll return the latest date when the server has create a value -- helpful for the next call.


Json for all results:

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
