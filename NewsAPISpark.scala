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
z.load("datastax:spark-cassandra-connector:2.0.7-s_2.10")




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
  
df.show

df.createOrReplaceTempView("GoogleNewAPIData");
val parmg = Map("url" -> googleNewsURL, "input" -> "GoogleNewAPIData", "method" -> "GET", "readTimeout" -> "10000", "connectionTimeout" -> "2000", "partitions" -> "10");
val newsDataDf = spark.read.format("org.apache.dsext.spark.datasource.rest.RestDataSource").options(parmg).load();
newsDataDf.createOrReplaceTempView("GoogleNewTempView");
var outDF = spark.sql("select output.articles from GoogleNewTempView");


/*******************************News API call**********************************/



/*******************************Apache Tika to extract text from htmls fetched and save to Cassandra**********************************/
def ParseURLContent(url: String) : String = {

    try { 
        var stream : InputStream = new URL(url).openStream()
        val handler : BodyContentHandler = new BodyContentHandler()
        val metadata : Metadata = new Metadata()
        val context : ParseContext = new ParseContext()
        val htmlparser : HtmlParser = new HtmlParser()
        htmlparser.parse(stream, handler, metadata, context)
        var content = handler.toString().trim().replaceAll(" +", " ").replaceAll("\n", " ").replaceAll("\\s", " ").replaceAll("\\s+", " ").filter(_ >= ' ').replaceAll("""(?m)\s+$""","").replaceAll("""^\s+(?m)""","").replaceAll("\\P{Print}", "").replaceAll("\\\\x\\p{XDigit}{2}", "")
        return content
        
   } catch {
        case e: Exception => return "ERROR FETCHING"
   }
}

def RemoveDescriptionSpaces(description: String) : String = {
    return description.toString().trim().replaceAll(" +", " ").replaceAll("\n", " ").replaceAll("\\s", " ").replaceAll("\\s+", " ").filter(_ >= ' ').replaceAll("""(?m)\s+$""","").replaceAll("""^\s+(?m)""","").replaceAll("\\P{Print}", "").replaceAll("\\\\x\\p{XDigit}{2}", "")
}


var explodedOut = outDF.select(explode(outDF("articles")))

import org.apache.spark.sql.functions._
val transformFunctiom = udf(ParseURLContent _)
val transformFunctiom2 = udf(RemoveDescriptionSpaces _)

val finalDataFrame = explodedOut.
withColumn("author", explodedOut("col.author")).
withColumn("description_notClean", explodedOut("col.description")).
withColumn("publishedat", explodedOut("col.publishedAt")).
withColumn("source_id", explodedOut("col.source.id")).
withColumn("source_name", explodedOut("col.source.name")).
withColumn("title", explodedOut("col.title")).
withColumn("url", explodedOut("col.url")).
withColumn("urltoimage", explodedOut("col.urlToImage")).
withColumn("content", transformFunctiom($"url")).
withColumn("description", transformFunctiom2($"description_notClean")).
drop("col").
drop("description_notClean")

// finalDataFrame.show()

finalDataFrame.write
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "mynewsapi", "keyspace" -> "uptick"))
  .save()



/*******************************Apache Tika to extract text from htmls fetched**********************************/
