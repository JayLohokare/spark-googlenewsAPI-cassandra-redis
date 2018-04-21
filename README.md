# spark-scalable-news-sentiment

Spark job to fetch latest news and run sentiment analysis on it

Uses a distributed REST API call framework to make parallel API calls to achieve scalability and highly available system.

Loads topics to fetch news for from Cassandra (Cassandra input column names should match newsapi.org API parameters).

Uses Apache Tika for extracting data from HTML pages fetched by newsapi.org

Uses deeplearning4j to run sentiment analysis on content fetched.


