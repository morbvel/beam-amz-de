package com.beamsuntory.amz.de.reporting.arap

import java.util.Calendar

import com.beamsuntory.amz.de.commons.UtilsAmzDE
import com.beamsuntory.bgc.commons.DataFrameUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


class ReportingArap(val utils: UtilsAmzDE){


  def mainExecution(): Unit = {

    reportingFinalArap
    reportingFinalArapInventory
  }

  def reportingFinalArap(): Unit = {

    val now = Calendar.getInstance()
    val year_window = now.get(Calendar.YEAR) - 1

    utils.spark.read.table(amzDEfun + "sales_diagnostic")
      .select("asin", "brand_partner", "day", "distributor_non_sourcing", "distributor_sourcing", "month",
      "prodh1", "prodh1_description", "prodh2", "prodh2_description", "prodh3", "prodh3_description", "prodh4", "prodh4_description",
      "prodh5", "prodh5_description", "prodh6", "prodh6_description", "product_title", "quarter", "size", "week", "year",
      "customer_returns", "customer_returns_sourcing", "free_replacements", "free_replacements_sourcing", "ordered_revenue",
      "ordered_revenue_last_year", "shipped_revenue", "shipped_revenue_last_year", "shipped_revenue_sourcing",
        "shipped_revenue_sourcing_last_year", "shipped_units", "shipped_units_last_year", "shipped_units_sourcing",
      "shipped_units_sourcing_last_year", "ordered_units", "ordered_units_last_year", "month_str", "volume_9L_shipped",
        "volume_9L_ordered", "volume_9L_shipped_sourcing", "volume_07L_shipped", "volume_07L_ordered", "volume_07L_shipped_sourcing",
        "volume_9L_shipped_last_year", "volume_9L_ordered_last_year", "volume_9L_shipped_sourcing_last_year", "volume_07L_shipped_last_year",
        "volume_07L_ordered_last_year", "volume_07L_shipped_sourcing_last_year", "last_twelve_month", "num_bottles", "volume_9L_customer_returns",
      "volume_07L_customer_returns", "volume_9L_customer_returns_sourcing", "volume_07L_customer_returns_sourcing", "volumen_9L_free_replacements",
        "volume_07L_free_replacements", "volumen_9L_free_replacements_sourcing", "volume_07L_free_replacements_sourcing")
      .writeHiveTable(amzDErep + "sales_diagnostic")

    utils.spark.read.table(amzDEfun + "geographic_sales_insights")
      .select("asin", "brand_partner", "city", "country", "plz", "day", "month", "prodh1", "prodh1_description",
        "prodh2", "prodh2_description", "prodh3", "prodh3_description", "prodh4", "prodh4_description", "state", "latitude", "longitude",
        "prodh5", "prodh5_description", "prodh6", "prodh6_description", "product_title", "quarter", "size", "week", "year",
        "ordered_revenue", "shipped_revenue", "shipped_units", "ordered_units", "month_str", "latitude_city", "longitude_city",
      "last_twelve_month",  "volume_9L_shipped", "volume_9L_ordered", "volume_07L_shipped", "volume_07L_ordered")
      .filter(col("year") >= year_window)
      .writeHiveTable(amzDErep + "geographic_sales_insights")
  }

  def reportingFinalArapInventory(): Unit = {


    utils.spark.read.table(amzDErep + "sales_diagnostic")
      .join(utils.spark.read.table(amzDEhar + "forecast_inventory_ordered" + "_stg")
        .select("asin", "day", "month", "year", "available_inventory", "last_day_inventory").distinct(),
        Seq("asin", "day", "month", "year"), "left_outer")
      .withColumn("volume_9L_inventory_level", col("num_bottles")*col("size")*col("available_inventory")/9000)
      .withColumn("volume_07L_inventory_level", col("num_bottles")*col("size")*col("available_inventory")/700)
      .writeHiveTable(amzDErep + "sales_diagnostic_inventory")

  }

}