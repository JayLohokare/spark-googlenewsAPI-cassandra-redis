import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import scalaj.http._
import org.w3c.dom._

import com.steadystate.css.dom.CSSStyleRuleImpl._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
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
import org.apache.tika.parser.html.BoilerpipeContentHandler
import org.apache.tika.parser.html.HtmlParser;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.extractors.CommonExtractors;
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput;
import de.l3s.boilerpipe.sax.HTMLDocument;
import de.l3s.boilerpipe.sax.HTMLFetcher;
import org.cyberneko.html;
import java.net.URL;
import org.xml.sax;
import com.gargoylesoftware.htmlunit

import com.intenthq.gander._

import com.gargoylesoftware.htmlunit._

import org.w3c.dom.ElementTraversal

import net.sourceforge.htmlunit._
        
        
object newsfetch
{

  

def RemoveDescriptionSpaces(description: String) : String = {
    return description.toString().trim().replaceAll(" +", " ").replaceAll("\n", " ").replaceAll("\\s", " ").replaceAll("\\s+", " ").filter(_ >= ' ').replaceAll("""(?m)\s+$""","").replaceAll("""^\s+(?m)""","").replaceAll("\\P{Print}", "").replaceAll("\\\\x\\p{XDigit}{2}", "")
}

def ParseURLContent(url: String) : String = {

    try {           
                val webClient : WebClient = new WebClient()
                webClient.setThrowExceptionOnScriptError(false);
                webClient.setCssEnabled(false);
                webClient.setAppletEnabled(false);
                webClient.setJavaScriptEnabled(false)

                
                val page : Page = webClient.getPage(url)
                val response : WebResponse = page.getWebResponse()
                val rawHTML : String = response.getContentAsString()
                val string = Gander.extract(rawHTML)
                
                val content : String =string.toString().trim().replaceAll(" +", " ").replaceAll("\n", " ").replaceAll("\\s", " ").replaceAll("\\s+", " ").filter(_ >= ' ').replaceAll("""(?m)\s+$""","").replaceAll("""^\s+(?m)""","").replaceAll("\\P{Print}", "").replaceAll("\\\\x\\p{XDigit}{2}", "").substring(100)
                
                return content
                
                
                // /***************Tika core*************************/
                // var stream : InputStream = new URL(url).openStream()
                // val handler : BodyContentHandler = new BodyContentHandler()
                // val metadata : Metadata = new Metadata()
                // val textExtractHandler : BoilerpipeContentHandler = new BoilerpipeContentHandler(handler)
                // val context : ParseContext = new ParseContext()
                // val htmlparser : HtmlParser = new HtmlParser()
                // htmlparser.parse(stream, textExtractHandler, metadata, context)
                
                // var content = textExtractHandler.toString().trim().replaceAll(" +", " ").replaceAll("\n", " ").replaceAll("\\s", " ").replaceAll("\\s+", " ").filter(_ >= ' ').replaceAll("""(?m)\s+$""","").replaceAll("""^\s+(?m)""","").replaceAll("\\P{Print}", "").replaceAll("\\\\x\\p{XDigit}{2}", "").substring(100)
                // var retString = Gander.extract(content).toString().trim().replaceAll(" +", " ").replaceAll("\n", " ").replaceAll("\\s", " ").replaceAll("\\s+", " ").filter(_ >= ' ').replaceAll("""(?m)\s+$""","").replaceAll("""^\s+(?m)""","").replaceAll("\\P{Print}", "").replaceAll("\\\\x\\p{XDigit}{2}", "").substring(100)
                // return retString
                
        
        
   } catch {
       case e: Exception =>{
           try{
               
                /***************BoilerPipe**************************/
                val htmlDoc : HTMLDocument  = HTMLFetcher.fetch(new URL(url))  
                val doc : TextDocument  = new BoilerpipeSAXInput(htmlDoc.toInputSource()).getTextDocument()
                var content : String = CommonExtractors.ARTICLE_EXTRACTOR.getText(doc).toString().trim().replaceAll(" +", " ").replaceAll("\n", " ").replaceAll("\\s", " ").replaceAll("\\s+", " ").filter(_ >= ' ').replaceAll("""(?m)\s+$""","").replaceAll("""^\s+(?m)""","").replaceAll("\\P{Print}", "").replaceAll("\\\\x\\p{XDigit}{2}", "")
                return content
                
           }
           catch{
                case e: Exception => return "ERROR FETCHING"
           }
           
       }
   }
}



def main(args:Array[String]) 
{
  
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()
import spark.sqlContext.implicits._
  
val cassandra_uri = "127.0.0.1"
val conf = new SparkConf()
.setMaster("local[2]")
.setAppName("UptickNewFetch")
.set("spark.app.id", "UptickNewFetc")
.set("spark.cassandra.connection.host", cassandra_uri)


val ssc = new SparkContext(conf)
//val spark = new SQLContext(ssc)
 
  
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
val outDF = spark.sql("select output.articles from GoogleNewTempView")

val explodedOut = outDF.select(explode(outDF("articles")))

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
  .mode(SaveMode.Append)
  .save()
  
}
}