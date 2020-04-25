package com.beamsuntory.amz.de.functional.ams

import com.beamsuntory.amz.de.commons.{ComplexAccumulatorV2, MyComplex, UtilsAmzDE}
import com.beamsuntory.amz.de.functional.arap.FunctionalAmsEdwColumns
import com.beamsuntory.bgc.commons.DataFrameUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


class FunctionalScenariosAms(val utils: UtilsAmzDE) extends FunctionalAmsEdwColumns {


  def mainExecution(): Unit = {
    createScenarios
    campaignScenarioMetrics
  }

  def createScenarios(): Unit = {
    val keywords_id: DataFrame = utils.spark.read.table(amzDEhar + "campaign_keywords").groupBy("keyword").agg((monotonically_increasing_id()).as("keywords_id"))
    val products_id: DataFrame = utils.spark.read.table(amzDEhar + "campaign_products").groupBy("product_title").agg((monotonically_increasing_id()).as("product_title_id"))

    val keywords_cost: DataFrame = utils.spark.read.table(amzDEhar + "campaign_keywords").groupBy("campaign_name", "start_date").agg(sum("keyword_bid").as("keywords_cost"))
    val sum_keywords: DataFrame = utils.spark.read.table(amzDEhar + "campaign_keywords").join(keywords_id, Seq("keyword"), "inner").dropDuplicates().groupBy("campaign_name", "start_date").agg(sum("keywords_id").as("sum_keywords_id"))
    val sum_products: DataFrame = utils.spark.read.table(amzDEhar + "campaign_products").join(products_id, Seq("product_title"), "inner").dropDuplicates().groupBy("campaign_name", "start_date").agg(sum("product_title_id").as("sum_product_title_id"))

    val sum_columns = List( col("keywords_cost"), col("sum_keywords_id"), col("sum_product_title_id"))
    val summary: DataFrame = utils.spark.read.table(amzDEhar + "campaigns_summary").select("campaign_name", "start_date").distinct()
      .join(sum_keywords, Seq("campaign_name", "start_date"), "inner").dropDuplicates()
      .join(sum_products, Seq("campaign_name", "start_date"), "inner").dropDuplicates()
      .join(keywords_cost, Seq("campaign_name", "start_date"), "inner").dropDuplicates()
      .withColumn("sum_values", sum_columns.reduce(_ + _))
      .select("campaign_name", "start_date", "sum_values")

    val window = Window.partitionBy("campaign_name").orderBy("start_date")
    spark.sparkContext.register(ComplexAccumulatorV2, "mycomplexacc")

    val new_scenario: DataFrame = summary.withColumn(
      "flag", when(
        col("sum_values").notEqual(lag(col("sum_values"), 1).over(window)) , lit(1)
      ).otherwise(lit(0)))

    val acc = new MyComplex(0)

    val func = udf((s: Int) => {
      if (s > 0) {
        acc.add(1)
        acc.x
      } else {
        acc.x
      }
    })

    val aux = new_scenario
      .withColumn("scenario", lit(0))
      .withColumn("scenario", func(col("flag")))
      .select("campaign_name", "start_date", "scenario")
      .orderBy("campaign_name", "start_date")

    val min_scenarios = aux.groupBy("campaign_name").agg(min("scenario").as("min_scenario"))

    val min_aux = aux.join(min_scenarios, Seq("campaign_name"), "inner").dropDuplicates()
    min_aux.withColumn("minimo_prueba", col("scenario")-col("min_scenario"))
      .drop("scenario", "min_scenario").withColumnRenamed("minimo_prueba", "scenario")
      .writeHiveTable(amzDEfun + "campaign_scenarios")

  }

  def campaignScenarioMetrics(): Unit = {

    val scenarios: DataFrame = utils.spark.read.table(amzDEfun + "campaign_scenarios")

    val full_union = utils.spark.read.table(amzDEfun + "keywords_sales_campaign")
      .filter(col("type_advertisement").isNotNull)
      .withColumn("keywords_products", lit("keywords"))
      .fullUnion(utils.spark.read.table(amzDEfun + "product_sales_campaigns")
        .filter(col("type_advertisement").isNotNull)
        .withColumn("keywords_products", lit("products")))
      .join(scenarios, Seq("campaign_name", "start_date"), "inner")

    full_union
      .filter(col("match_type").isNotNull)
      .select("scenario","campaign_name", "status", "type_campaign", "keyword", "type_advertisement", "match_type",
        "start_date", "day", "week", "month", "month_str", "quarter", "year", "brand_partner", "keyword_bid",
        "prodh1_description", "prodh2_description", "prodh3_description", "prodh4_description", "prodh5_description", "prodh6_description",
        "marketing_campaign_revenue", "cost_per_click", "impressions",
        "clicks", "cost_of_campaign", "marketing_campaign_orders", "budget")
      .writeHiveTable(amzDEfun + "campaign_analytics")

  }

}