
package com.uptick.newfetch

/*BoilerPipe imports*/
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import scalaj.http._
import org.w3c.dom._
import com.steadystate.css.dom.CSSStyleRuleImpl._
import org.apache.spark.sql.SaveMode
import org.apache.spark.input.PortableDataStream
import java.io._
import java.net.URL;
import de.l3s.boilerpipe.BoilerpipeExtractor;
import de.l3s.boilerpipe.extractors.CommonExtractors;
import de.l3s.boilerpipe.sax.HTMLHighlighter;
import de.l3s.boilerpipe.extractors.ArticleExtractor;   
import de.l3s.boilerpipe.extractors.ArticleSentencesExtractor;
import de.l3s.boilerpipe.extractors.DefaultExtractor;   
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput;
import de.l3s.boilerpipe.sax.HTMLFetcher;
import org.xml.sax.InputSource;
import com.datastax.spark.connector._
import org.w3c.dom._
import com.steadystate.css.dom.CSSStyleRuleImpl._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.cyberneko.html
import java.net.URL
import org.xml.sax
import com.gargoylesoftware.htmlunit
import java.io._
import java.net.URL;
import com.intenthq.gander._
import com.gargoylesoftware.htmlunit._
import org.w3c.dom.ElementTraversal
import net.sourceforge.htmlunit._
import org.apache.spark.sql.SparkSession



        

      
object newsfetch
{

  

def RemoveDescriptionSpaces(description: String) : String = {
    return description.toString().trim().replaceAll(" +", " ").replaceAll("\n", " ").replaceAll("\\s", " ").replaceAll("\\s+", " ").filter(_ >= ' ').replaceAll("""(?m)\s+$""","").replaceAll("""^\s+(?m)""","").replaceAll("\\P{Print}", "").replaceAll("\\\\x\\p{XDigit}{2}", "")
}

def ParseURLContent(url: String) : String = {

    try {        /*   
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
*/
		var url2 : URL  = new URL(url)
                var is : InputSource = HTMLFetcher.fetch(url2).toInputSource()
                var in : BoilerpipeSAXInput = new BoilerpipeSAXInput(is)
                var doc : TextDocument = in.getTextDocument()
                return ArticleSentencesExtractor.INSTANCE.getText(doc).toString().trim().replaceAll(" +", " ").replaceAll("\n", " ").replaceAll("\\s", " ").replaceAll("\\s+", " ").filter(_ >= ' ').replaceAll("""(?m)\s+$""","").replaceAll("""^\s+(?m)""","").replaceAll("\\P{Print}", "").replaceAll("\\\\x\\p{XDigit}{2}", "")
                
                
                
        
   } catch {
       case e: Exception =>{
return "Error"
         
       }
   }
}



def main(args:Array[String]) 
{
try{
  

//import org.apache.spark.sql.SparkSession
/*
val spark = SparkSession.builder
  .master("local")
  .appName("NewsFetch")
  .getOrCreate()*/

//import spark.implicits._
/**
val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("NewsFetch")
      .set("spark.app.id", "NewsFetch")

val sc = new SparkContext(conf)
val sqc = new SQLContext(sc)
val spark = sqc.spark_session
  
val cassandra_uri = "127.0.0.1"
**/



val spark = SparkSession.builder
.master("local[2]")
      .appName("NewsFetch")
.enableHiveSupport()
.getOrCreate()

import spark.sqlContext.implicits._


val googleNewsURL = "https://newsapi.org/v2/everything"

val df = spark
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "cryptonews", "keyspace" -> "uptick" ))
  .load()
df.show()


df.createOrReplaceTempView("GoogleNewAPIData");
val parmg = Map("url" -> googleNewsURL, "input" -> "GoogleNewAPIData", "method" -> "GET", "readTimeout" -> "1000000", "connectionTimeout" -> "200000", "partitions" -> "10")

var newsDataDf = spark.read.format("org.apache.dsext.spark.datasource.rest.RestDataSource").options(parmg).load()

newsDataDf.createOrReplaceTempView("GoogleNewTempView");
val outDF = spark.sql("select output.articles from GoogleNewTempView")

val explodedOut = outDF.select(explode(outDF("articles")))
outDF.show()

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

finalDataFrame.write
  .format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "mynewsapi", "keyspace" -> "uptick"))
  .mode(SaveMode.Append)
  .save()



} catch {
          case e: Exception => 
{
println("Something went wrong")
print(e.getStackTrace().toString())

}
}


  
}



}
