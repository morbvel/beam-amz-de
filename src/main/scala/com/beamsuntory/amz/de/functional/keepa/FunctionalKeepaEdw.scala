package com.beamsuntory.amz.de.functional.keepa

import com.beamsuntory.amz.de.commons.UtilsAmzDE
import com.beamsuntory.bgc.commons.DataFrameUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.util.Calendar

import org.apache.spark.sql.types.TimestampType


class FunctionalKeepaEdw(val utils: UtilsAmzDE) extends FunctionalKeepaEdwColumns {


  def mainExecution(): Unit = {
    functionalHierarchy
  }

  def functionalHierarchy(): Unit = {

    val now = Calendar.getInstance()
    val year_window = now.get(Calendar.YEAR) - 1

    val asin_hierarchy: DataFrame = utils.spark.read.table(amzDEhar + "hierarchy_asin_amz_de")
      .select(selHierarchyColumns: _*)
      .distinct()

    val keepa_pricing_review: DataFrame = utils.spark.read.table(amzDEhar + "keepa_pricing_full_dates")
      .withColumn("month_str",utils.mappingMonthNameUDF(col("month")))

    val sales_products: DataFrame = utils.spark.read.table(amzDErep + "sales_diagnostic_inventory")
      .filter(col("year") >= year_window)
      .withColumn("timestamp", utils.date_transform(concat_ws("-", col("year"), col("month"), col("day"))).cast(TimestampType))

    val sales_products_customer_replacements: DataFrame = sales_products.groupBy("asin", "product_title", "year")
      .agg(sum(col("customer_returns")).as("customer_returns"),
        sum(col("free_replacements")).as("free_replacements"))
      .withColumn("type_info", lit("data_year"))

    val marketing_revenue: DataFrame = cleaningMarketingData()

    utils.fillwithUnassigned(utils.spark.read.table(amzDEhar + "keepa_pricing_full_dates")
      .fullUnion(sales_products.select(selSalesColumns: _*))
      .fullUnion(sales_products.select(selSalesLYColumns: _*))
      .fullUnion(sales_products.select(selSalesAvailableInventoryColumns: _*))
      .withColumn("type_info", lit("graph"))
      .fullUnion(sales_products_customer_replacements.select(selSalesCustomerReturnColumns: _*))
      .fullUnion(sales_products_customer_replacements.select(selSalesFreeReplacementsColumns: _*))
      .fullUnion(marketing_revenue)
      .join(asin_hierarchy, Seq("asin"), "left_outer"))
      .fullUnion(utils.spark.read.table(amzDEraw + "products_images").withColumn("type_info", lit("image")))
      .writeHiveTable(amzDEfun + "keepa_pricing_hierarchy")

  }

  def cleaningMarketingData(): DataFrame = {

    utils.spark.read.table(amzDEfun  + "product_sales_campaigns")
      .withColumnRenamed("product_title", "title")
      .filter(col("campaign_name") =!= "Without Campaign")
      .groupBy("asin", "title", "month", "month_str", "year")
      .agg(sum(col("impressions")).as("impressions"),
        sum(col("cost_of_campaign")).as("cost_of_campaign"),
        sum(col("marketing_campaign_revenue")).as("marketing_campaign_revenue"),
        sum(col("marketing_campaign_orders")).as("marketing_campaign_orders"),
        sum(col("clicks")).as("clicks"),
        countDistinct(col("campaign_name")).as("num_campaigns")
      )
      .withColumn("type_info", lit("marketing"))

  }

}
