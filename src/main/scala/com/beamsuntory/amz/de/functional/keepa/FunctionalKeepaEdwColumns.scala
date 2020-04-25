package com.beamsuntory.amz.de.functional.keepa

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

trait FunctionalKeepaEdwColumns {


  val selHierarchyColumns: Seq[Column] = Seq(
    col("asin"),
    col("prodh1"),
    col("prodh1_description"),
    col("prodh2"),
    col("prodh2_description"),
    col("prodh3"),
    col("prodh3_description"),
    col("prodh4"),
    col("prodh4_description"),
    col("prodh5"),
    col("prodh5_description"),
    col("prodh6"),
    col("prodh6_description"),
    col("size"),
    col("brand_partner")
  )

  val selSalesColumns: Seq[Column] = Seq(
    col("asin"),
    col("product_title").as("title"),
    col("day"),
    col("month"),
    col("month_str"),
    col("year"),
    col("week"),
    col("timestamp"),
    col("ordered_revenue").as("data_value"),
    lit("ORDERED_REVENUE").as("data_type"),
    col("ordered_units")
  )

  val selSalesLYColumns: Seq[Column] = Seq(
    col("asin"),
    col("product_title").as("title"),
    col("day"),
    col("month"),
    col("month_str"),
    col("year"),
    col("week"),
    col("timestamp"),
    col("ordered_revenue_last_year").as("data_value"),
    lit("ORDERED_REVENUE_LY").as("data_type"),
    col("ordered_units_last_year")
  )

  val selSalesCustomerReturnColumns: Seq[Column] = Seq(
    col("asin"),
    col("product_title").as("title"),
    col("year"),
    col("customer_returns").as("data_value"),
    lit("CUSTOMER RETURN").as("data_type"),
    col("type_info")

  )

  val selSalesFreeReplacementsColumns: Seq[Column] = Seq(
    col("asin"),
    col("product_title").as("title"),
    col("year"),
    col("free_replacements").as("data_value"),
    lit("FREE REPLACEMENTS").as("data_type"),
    col("type_info")
  )

  val selSalesAvailableInventoryColumns: Seq[Column] = Seq(
    col("asin"),
    col("product_title").as("title"),
    col("day"),
    col("month"),
    col("month_str"),
    col("year"),
    col("week"),
    col("timestamp"),
    col("available_inventory").as("data_value"),
    lit("AVAILABLE INVENTORY").as("data_type")
  )


}
