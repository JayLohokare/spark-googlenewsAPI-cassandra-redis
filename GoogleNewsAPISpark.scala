//Dependencies in dependencies folder

val googleNewsURL = "https://newsapi.org/v2/everything"


val input1 = ("1eb26e8ea0e4424ea969bb17d1378aef", "bitcoin")
val input2 = ("1eb26e8ea0e4424ea969bb17d1378aef", "ethereum")
val input3 = ("1eb26e8ea0e4424ea969bb17d1378aef", "ripple")

val inputRdd = sc.parallelize(Seq(input1, input2, input3))

val inputKey1 = "apiKey"
val inputKey2 = "q"

val inputDf = inputRdd.toDF(inputKey1, inputKey2)

inputDf.createOrReplaceTempView("GoogleNewAPIData")

val parmg = Map("url" -> googleNewsURL, "input" -> "GoogleNewAPIData", "method" -> "GET", "readTimeout" -> "10000", "connectionTimeout" -> "2000", "partitions" -> "10")

val newsDataDf = spark.read.format("org.apache.dsext.spark.datasource.rest.RestDataSource").options(parmg).load()

newsDataDf.printSchema 

newsDataDf.createOrReplaceTempView("GoogleNewTempView")

val outDF = spark.sql("select output from GoogleNewTempView").show()

// spark.sql("select inline(articles) from GoogleNewTempView2").show()
