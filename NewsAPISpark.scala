//Dependencies in dependencies folder

//Load dependencies on Zepplin
%dep 
z.load("/home/jay/Desktop/spark-2.3.0-bin-hadoop2.7/jars/spark-cassandra-connector_2.11-2.0.7.jar") 
z.load("/home/jay/Desktop/spark-2.3.0-bin-hadoop2.7/jars/spark-datasource-rest_2.11-2.1.0-SNAPSHOT.jar")
z.load("/home/jay/Desktop/spark-2.3.0-bin-hadoop2.7/jars/scalaj-http_2.10-2.3.0.jar")
z.load("/home/jay/Desktop/spark-2.3.0-bin-hadoop2.7/jars/tika-serialization-1.17.jar")
z.load("/home/jay/Desktop/spark-2.3.0-bin-hadoop2.7/jars/tika-core-1.17.jar")
z.load("/home/jay/Desktop/spark-2.3.0-bin-hadoop2.7/jars/tika-app-1.17.jar")
z.load("/home/jay/Desktop/spark-2.3.0-bin-hadoop2.7/jars/tika-bundle-1.17.jar")
z.load("/home/jay/Desktop/spark-2.3.0-bin-hadoop2.7/jars/tika-parsers-1.17.jar")



//Imports
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import scalaj.http._
import org.apache.spark.input.PortableDataStream
import org.apache.tika.metadata._
import org.apache.tika.parser._
import org.apache.tika.sax.WriteOutContentHandler
import java.io._
import org.apache.tika.Tika
import org.apache.tika.language.LanguageIdentifier
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.{AutoDetectParser, Parser, ParseContext}
import org.apache.tika.sax.BodyContentHandler
import org.apache.tika.parser.html.HtmlParser;
import java.net.URL;



/*******************************News API call**********************************/
val googleNewsURL = "https://newsapi.org/v2/everything"
//This can be any URL to make the REST API call

val df = spark
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "cryptonews", "keyspace" -> "uptick" ))
  .load()
//Loading parameters from Cassandra
  
df.show

df.createOrReplaceTempView("GoogleNewAPIData")
val parmg = Map("url" -> googleNewsURL, "input" -> "GoogleNewAPIData", "method" -> "GET", "readTimeout" -> "10000", "connectionTimeout" -> "2000", "partitions" -> "10")
val newsDataDf = spark.read.format("org.apache.dsext.spark.datasource.rest.RestDataSource").options(parmg).load()
newsDataDf.printSchema 
newsDataDf.createOrReplaceTempView("GoogleNewTempView")

val outDF = spark.sql("select output.articles.url from GoogleNewTempView")

/*******************************News API call**********************************/



/*******************************Apache Tika to extract text from htmls fetched**********************************/
//Run this part over the data fetched from NewsApi.org
var stream : InputStream = new URL("http://jaylohokare.com/index.html").openStream()
val handler : BodyContentHandler = new BodyContentHandler()
val metadata : Metadata = new Metadata()
val context : ParseContext = new ParseContext()
val htmlparser : HtmlParser = new HtmlParser()
htmlparser.parse(stream, handler, metadata, context);
println(handler.toString())

/*******************************Apache Tika to extract text from htmls fetched**********************************/
