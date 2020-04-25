package com.beamsuntory.amz.de.functional.ams

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

trait FunctionalRadarAmsColumns {


  val selProdcutKeywordColumns: Seq[Column] = Seq(
    col("marketing_campaign_revenue"),
    col("marketing_campaign_orders"),
    col("impressions"),
    col("type_campaign"),
    col("cost_of_campaign"),
    col("year"),
    col("quarter"),
    col("campaign_name"),
    col("clicks"),
    col("week"),
    col("budget"),
    col("day"),
    col("month"),
    col("month_str")
  )

  val selBaseColumns: Seq[String] = Seq(
    "type_campaign",
    "year",
    "quarter",
    "campaign_name",
    "week",
    "day",
    "month",
    "month_str",
    "prodh2_description",
    "prodh3_description",
    "prodh4_description",
    "brand_partner"
  )


}
