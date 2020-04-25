package com.beamsuntory.amz.de.functional.ams

import com.beamsuntory.amz.de.commons.{ComplexAccumulatorV2, MyComplex, UtilsAmzDE}
import com.beamsuntory.amz.de.functional.arap.FunctionalAmsEdwColumns
import com.beamsuntory.bgc.commons.DataFrameUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext


class FunctionalAmsEdw(val utils: UtilsAmzDE) extends FunctionalAmsEdwColumns {


  def mainExecution(): Unit = {
    functionalProductSalesEdw
    functionalKeywordSales
    functionalUnionProductKeyword
  }

  def functionalProductSalesEdw(): Unit = {

    val asin_hierarchy: DataFrame = utils.spark.read.table(amzDEfun + "sales_diagnostic")
      .select("asin", "prodh1", "prodh1_description", "prodh2", "prodh2_description",
        "prodh3", "prodh3_description", "prodh4", "prodh4_description", "prodh5_description", "prodh5",
        "prodh6", "prodh6_description", "brand_partner")
      .distinct()

    val sales_filtered: DataFrame = utils.spark.read.table(amzDEfun + "sales_diagnostic")
      .select(col("ordered_revenue"), col("ordered_units"),
        col("avg_sales_price_ordered"), col("asin"), col("start_date"))

    val product_campaigns_ean: DataFrame = utils.spark.read.table(amzDEhar + "campaign_products")
      .join(sales_filtered, Seq("asin", "start_date"), "left_outer")
      .withColumn("marketing_campaign_units", round(col("marketing_campaign_revenue")/col("avg_sales_price_ordered")))

    val product_campaign_grouped: DataFrame = calculatedWithoutCampaign(product_campaigns_ean)

    val product_sales_campaigns: DataFrame = product_campaigns_ean.fullUnion(product_campaign_grouped)
      .as("product_campaign_all")
      .join(asin_hierarchy,
        Seq("asin"), "left_outer")

    product_sales_campaigns
      .withColumn("asin", trim(col("asin")))
      .withColumn("prodh1_description",when( col("prodh1_description").isNull, lit("UNASSIGNED") ).otherwise(col("prodh1_description")))
      .withColumn("prodh2_description",when( col("prodh2_description").isNull, lit("UNASSIGNED") ).otherwise(col("prodh2_description")))
      .withColumn("prodh3_description",when( col("prodh3_description").isNull, lit("UNASSIGNED FAMILY") ).otherwise(col("prodh3_description")))
      .withColumn("prodh4_description",when( col("prodh4_description").isNull, lit("UNASSIGNED BRAND") ).otherwise(col("prodh4_description")))
      .withColumn("prodh5_description",when( col("prodh5_description").isNull, lit("UNNASSIGNED") ).otherwise(col("prodh5_description")))
      .withColumn("prodh6_description",when( col("prodh6_description").isNull, lit("UNNASSIGNED") ).otherwise(col("prodh6_description")))
      .withColumn("brand_partner", when(col("brand_partner").isNull, "UNASSIGNED").otherwise(upper(col("brand_partner"))))
      .writeHiveTable(amzDEfun  + "product_sales_campaigns")

    val hierarchy_campaigns = product_sales_campaigns
        .where(col("campaign_name") =!= "Without Campaign")
      .select("campaign_name", "prodh1", "prodh1_description", "prodh2", "prodh2_description",
        "prodh3", "prodh3_description", "prodh4", "prodh4_description", "brand_partner")
      .distinct()

    normalizeHierarchyCampaign(hierarchy_campaigns)
      .writeHiveTable(amzDEfun + "hierarchy_campaigns")
  }

  def normalizeHierarchyCampaign(hierarchy_campaigns: DataFrame): DataFrame = {

    val hierarchy_campaigns_lvl1 = hierarchy_campaigns.select("campaign_name", "prodh1_description")
      .groupBy("campaign_name")
      .agg(count("prodh1_description").as("count_lvl_1"))

    val hierarchy_campaigns_lvl2 = hierarchy_campaigns.select("campaign_name", "prodh2_description")
      .groupBy("campaign_name")
      .agg(count("prodh2_description").as("count_lvl_2"))

    val hierarchy_campaigns_lvl3 = hierarchy_campaigns.select("campaign_name", "prodh3_description")
      .groupBy("campaign_name")
      .agg(count("prodh3_description").as("count_lvl_3"))

    val hierarchy_campaigns_lvl4 = hierarchy_campaigns.select("campaign_name", "prodh4_description")
      .groupBy("campaign_name")
      .agg(count("prodh4_description").as("count_lvl_4"))

    val hierarchy_campaigns_brand_partner = hierarchy_campaigns.select("campaign_name", "brand_partner")
      .groupBy("campaign_name")
      .agg(count("brand_partner").as("count_brand_partner"))

    hierarchy_campaigns.join(hierarchy_campaigns_lvl1, Seq("campaign_name"), "inner")
      .join(hierarchy_campaigns_lvl2, Seq("campaign_name"), "inner")
      .join(hierarchy_campaigns_lvl3, Seq("campaign_name"), "inner")
      .join(hierarchy_campaigns_lvl4, Seq("campaign_name"), "inner")
      .join(hierarchy_campaigns_brand_partner, Seq("campaign_name"), "inner")
      .withColumn("prodh1_description", when(col("count_lvl_1") > lit(1), "MIXED")
      .otherwise(col("prodh1_description")))
      .withColumn("prodh2_description", when(col("count_lvl_2") > lit(1), "MIXED FAMILIES")
        .otherwise(col("prodh2_description")))
      .withColumn("prodh3_description", when(col("count_lvl_3") > lit(1), "MIXED BRANDS")
        .otherwise(col("prodh3_description")))
      .withColumn("prodh4_description", when(col("count_lvl_4") > lit(1), "OTHERS")
        .otherwise(col("prodh4_description")))
      .withColumn("brand_partner", when(col("count_brand_partner") > lit(1), "OTHERS")
        .otherwise(col("brand_partner")))
      .select("campaign_name", "prodh1_description", "prodh2_description", "prodh3_description",
        "prodh4_description", "brand_partner")
      .dropDuplicates()

  }

  def calculatedWithoutCampaign(product_campaigns_ean: DataFrame): DataFrame = {
    product_campaigns_ean
      .select("start_date", "asin", "product_title", "month_str","month", "year", "day", "week", "quarter",
        "marketing_campaign_revenue", "marketing_campaign_orders", "ordered_revenue", "ordered_units", "avg_sales_price_ordered",
        "marketing_campaign_units")
      .withColumn("number_of_campaigns", lit(1))
      .groupBy("start_date", "asin", "product_title", "month_str","month", "year", "day", "week", "quarter", "ordered_revenue", "ordered_units",
        "avg_sales_price_ordered")
      .agg(sum("marketing_campaign_revenue").as("marketing_campaign_revenue"),
        sum("marketing_campaign_orders").as("marketing_campaign_orders"),
        sum("number_of_campaigns").as("number_of_campaigns"),
        sum("marketing_campaign_units").as("marketing_campaign_units"))
      .withColumn("campaign_name", lit("Without Campaign"))
      .withColumn("status", lit("UNDETERMINED"))
      .withColumn("marketing_campaign_revenue", col("ordered_revenue") - col("marketing_campaign_revenue"))
      .withColumn("marketing_campaign_orders", col("ordered_units") - col("marketing_campaign_units"))
  }

  def functionalKeywordSales(): Unit = {

    val hierarchy_campaigns: DataFrame = utils.spark.read.table(amzDEfun + "hierarchy_campaigns")

    utils.spark.read.table(amzDEhar + "campaign_keywords")
      .join(hierarchy_campaigns ,Seq("campaign_name") ,"left_outer")
      .withColumn("item_name", trim(split(col("campaign_name"), "-").getItem(1)))
      .withColumn("campaign_hierarchy", utils.mapping_hierarchy_campaigns_hsa(col("item_name")))
      .withColumn("prodh2_description", when(col("prodh2_description").isNotNull, col("prodh2_description"))
        .otherwise(split(col("campaign_hierarchy"), "-").getItem(0)))
      .withColumn("prodh3_description", when(col("prodh3_description").isNotNull, col("prodh3_description"))
        .otherwise(split(col("campaign_hierarchy"), "-").getItem(1)))
      .withColumn("prodh4_description", when(col("prodh4_description").isNotNull, col("prodh4_description"))
        .otherwise(split(col("campaign_hierarchy"), "-").getItem(2)))
      .withColumn("brand_partner", when(col("brand_partner").isNotNull, col("brand_partner"))
        .otherwise(split(col("campaign_hierarchy"), "-").getItem(3)))
      .withColumn("brand_partner", when(col("brand_partner")==="THIRD PARTY", lit("THIRD-PARTY"))
        .otherwise(col("brand_partner")))
      .writeHiveTable(amzDEfun + "keywords_sales_campaign")

  }

  def functionalUnionProductKeyword(): Unit = {
    utils.spark.read.table(amzDEfun + "keywords_sales_campaign")
      .withColumn("keywords_products", lit("keywords"))
      .fullUnion(utils.spark.read.table(amzDEfun + "product_sales_campaigns")
      .withColumn("keywords_products", lit("products")))
      .writeHiveTable(amzDEfun + "product_keyword_campaign_old")
  }
}
