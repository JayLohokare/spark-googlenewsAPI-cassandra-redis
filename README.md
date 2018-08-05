Spark job to fetch real time / historic news and run sentiment analysis on it

Uses 
```
NewsAPI.org
```
for real time news feeds. 

The project uses a distributed REST API call framework based on scala to allow handling multiple news topics. 
See https://github.com/sourav-mazumder/Data-Science-Extensions for more details about the framework.

The project uses Cassandra for metadata source and news data storage.

(Install SBT and then) To generate the Spark jar file, run -

```
sbt package 
```

The project requires external Spark-cassandra connector jar. This jar is passed as a command-line argument to spark-submit. Git clone this project in spark installation location and run the following command:

```
sudo bin/spark-submit \
--jars location_to_cassandra_jar/spark-cassandra-connector_2.11-2.3.0.jar \
--class com.uptick.newfetch.newsfetch \
--master ENTER_MASTER_TYPE \
--deploy-mode ENTER_DEPLOY_MODE \
--conf spark.cassandra.connection.host=Cassandra_IP_Address \
location_of_jar/newsfetch_2.11-1.0.jar
```

Ensure that the dependencies versions are correct. The project currently uses Scala 2.11 with Spark 2.3

Once the news link is fetched from newsapi.org, the project uses various methods to extract the main article content from html pages. 

The project uses Cassandra keyspace called 'uptick'. The news topics are loaded from table 'cryptonews'. The table 'cryptonews' contains columns 'q', 'language', 'apikey' (Naming convention follows the API parameters for newsapi.org).

The output of the application is stored in table 'mynewsapi'


//Under development - UDF for Tensorflow integration to get sentiment and relevance score from the extracted text
