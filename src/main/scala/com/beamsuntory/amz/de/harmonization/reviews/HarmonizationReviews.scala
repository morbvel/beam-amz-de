package com.beamsuntory.amz.de.harmonization.reviews

import com.beamsuntory.amz.de.commons.UtilsAmzDE
import com.beamsuntory.amz.de.harmonization.reviews.HarmonizationReviewsColumns
import com.beamsuntory.bgc.commons.DataFrameUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._


//IMPORT FOR APIR REST CONNECTIONS
import org.apache.commons._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import java.util.ArrayList

import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import scala.io.Source


class HarmonizationReviews(val utils: UtilsAmzDE) extends HarmonizationReviewsColumns with Serializable{


  def mainExecution(): Unit = {
    harmonizationReviewsSentiments()
  }


  def harmonizationReviewsSentiments(): Unit = {
    // Reading updated reviews and adding their hierarchy
    val rev: DataFrame = utils.spark.read.table(amzDEraw + "amz_review_products")
    val form_rev: DataFrame = formatReviews(rev)

    val asin_hierarchy: DataFrame = utils.spark.read.table(amzDEhar + "hierarchy_asin_amz_de")
      .select(selHierarchyColumns: _*)
      .distinct()

    val reviews_hierarchy = form_rev.join(asin_hierarchy, Seq("asin"), "left_outer")
    reviews_hierarchy.writeHiveTable(amzDEhar + "reviews_hierarchy")

    // Reading latest sentiment and translation analysis
    val analysis: DataFrame = readOrCreateAnalysis()

    // Group by id_reviews in reviews_hierarchy and analysis
    val r: DataFrame = reviews_hierarchy.groupBy(col("id_review")).agg(count("id_review")).select("id_review")
    val a: DataFrame = analysis.groupBy(col("id_review")).agg(count("id_review")).select("id_review")
    val diffs: DataFrame = r.join(a, Seq("id_review"), "left_anti")
    r.fullUnion()

    val id_to_analyze: DataFrame = diffs.join(reviews_hierarchy, Seq("id_review"), "left_outer").select(col("id_review"), col("review_body"))


    // UDF and method to send to API Rest
    val getSentiment = (text: String, opt: String) => {

      val post = new HttpPost("https://sentiments-translation-dot-bsddl-161716.appspot.com/create_sentiment_translate")
      val nameValuePairs = new ArrayList[NameValuePair]()

      nameValuePairs.add(new BasicNameValuePair("text", text))
      nameValuePairs.add(new BasicNameValuePair("opt", opt))

      post.setEntity(new UrlEncodedFormEntity(nameValuePairs))

      val client = new DefaultHttpClient
      val response = client.execute(post)

      val entity = response.getEntity
      var content = ""

      if (entity != null) {
        val inputStream = entity.getContent()
        content = Source.fromInputStream(inputStream).getLines().mkString
        inputStream.close
      }

      content
    }
    val getSentimentUDF = udf(getSentiment)

    val distincts_ids: DataFrame = id_to_analyze.groupBy(col("id_review")).agg(count("id_review")).select("id_review")

    // Getting metrics
    id_to_analyze
      .join(distincts_ids, Seq("id_review"), "inner")
      .withColumn("score", getSentimentUDF(col("review_body"), lit("score")) )
      .filter(col("score").isNotNull and col("score").notEqual("<html><head><title>502 Bad Gateway</title></head><body><center><h1>502 Bad Gateway</h1></center><hr><center>nginx</center></body></html>"))
      .select("id_review", "score").distinct()
      .writeHiveTable(amzDEhar + "sentiment_score")

    id_to_analyze
      .join(distincts_ids, Seq("id_review"), "inner")
      .withColumn("magnitude", getSentimentUDF(col("review_body"), lit("magnitude")) )
      .filter(col("magnitude").isNotNull and col("magnitude").notEqual("<html><head><title>502 Bad Gateway</title></head><body><center><h1>502 Bad Gateway</h1></center><hr><center>nginx</center></body></html>"))
      .select("id_review", "magnitude").distinct()
      .writeHiveTable(amzDEhar + "sentiment_magnitude")

    id_to_analyze
      .join(distincts_ids, Seq("id_review"), "inner")
      .withColumn("translation", getSentimentUDF(col("review_body"), lit("translation")) )
      .filter(col("translation").isNotNull and col("translation").notEqual("<html><head><title>502 Bad Gateway</title></head><body><center><h1>502 Bad Gateway</h1></center><hr><center>nginx</center></body></html>"))
      .select("id_review", "translation").distinct()
      .writeHiveTable(amzDEhar + "sentiment_translation")

    // Join metrics to sentiment_analysis original table
    val score: DataFrame = utils.spark.read.table(amzDEhar + "sentiment_score")
    val magnitude: DataFrame = utils.spark.read.table(amzDEhar + "sentiment_magnitude")
    val translation: DataFrame = utils.spark.read.table(amzDEhar + "sentiment_translation")

    val aux: DataFrame = score.join(magnitude, Seq("id_review"), "inner").distinct()
    val aux2: DataFrame = aux.join(translation, Seq("id_review"), "inner").distinct()

    val ids_to_join: DataFrame = aux2.select("id_review", "score", "magnitude", "translation")

    analysis.join(ids_to_join, Seq("id_review"), "left_outer").writeHiveTable(amzDEhar + "sentiment_analysis")


  }

  def formatReviews(df: DataFrame): DataFrame = {

    val months: Seq[String] = Seq("Januar", "Februar", "März", "April", "Mayo", "Juni", "Juli", "August", "September", "Oktober", "November", "Dezember")

    val getIndex = (month: String) => months.indexOf(month) + 1
    val getIndexUDF = udf(getIndex)

    val df_aux:DataFrame = df.filter(col("asin").notEqual("asin"))
      .withColumn("_tmp", split(col("date"), " "))
      .withColumn("_day", col("_tmp").getItem(0))
      .withColumn("_month", col("_tmp").getItem(1))
      .withColumn("year", col("_tmp").getItem(2))
      .withColumn("day",regexp_replace(col("_day"), "\\.", " "))
      .withColumn("month", getIndexUDF(col("_month")))

    df_aux.select(selectReviews: _*)
      .withColumnRenamed("_verified_buy", "verified_buy")
      .withColumnRenamed("_rating", "rating")
  }

  def readOrCreateAnalysis(): DataFrame = {
    utils.spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS amz_de_harmonization.sentiment_analysis(" +
      "id_review STRING," +
      "score FLOAT," +
      "magnitude FLOAT," +
      "translation STRING)" +
      "COMMENT 'row data csv'" +
      "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'" +
      "WITH SERDEPROPERTIES (" +
      "'field.delim'='\\;'," +
      "'serialization.format'='\\;'," +
      "'quoteChar' = '\\\"')" +
      "STORED AS INPUTFORMAT" +
      "'org.apache.hadoop.mapred.TextInputFormat'" +
      "OUTPUTFORMAT" +
      "'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'"+
      "location 'bstdl-raw-de-amz/sentiment_analysis'")

    utils.spark.read.table(amzDEhar + "sentiment_analysis")
  }

}
