package com.beamsuntory.amz.de.functional.keepa

import java.util.Calendar

import com.beamsuntory.amz.de.commons.UtilsAmzDE
import com.beamsuntory.bgc.commons.DataFrameUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType


class FunctionalGeographicKeepa(val utils: UtilsAmzDE) extends  FunctionalKeepaEdwColumns {


  def mainExecution(): Unit = {
    functionalGeographicKeepa
  }

  def functionalGeographicKeepa(): Unit = {
    val now = Calendar.getInstance()
    val year_window = now.get(Calendar.YEAR) - 1

    val asin_hierarchy: DataFrame = utils.spark.read.table(amzDEhar + "hierarchy_asin_amz_de")
      .select(selHierarchyColumns: _*)
      .distinct()

    val geographic_sales = utils.spark.read.table(amzDEfun + "geographic_sales_insights")
      .select("asin", "city", "country", "plz", "month", "product_title", "year",
        "ordered_revenue", "ordered_units", "month_str", "latitude_city", "longitude_city",  "latitude", "longitude")
      .filter(col("year") >= year_window)
      .withColumnRenamed("product_title", "title")
      .groupBy("asin", "city", "country", "plz", "month", "title", "year",
        "month_str", "latitude_city", "longitude_city",  "latitude", "longitude")
      .agg(sum("ordered_revenue").as("ordered_revenue"),
        sum("ordered_units").as("ordered_units"))

    geographic_sales.join(asin_hierarchy, Seq("asin"), "left_outer")
      .writeHiveTable(amzDEfun + "geographic_sales_keepa")

  }


}
