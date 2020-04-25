package com.beamsuntory.amz.de.harmonization.ams

import com.beamsuntory.amz.de.commons.UtilsAmzDE
import com.beamsuntory.bgc.commons.DataFrameUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


class HarmonizationAms(val utils: UtilsAmzDE) {


  def mainExecution(): Unit = {
    // Summary table cleanance
    harmonizationCleanCampaignsSummary()

    // Product + Summary
    harmonizationJoinProductSummary()

    // Keywords + Summary
    harmonizationJoinKeywordsSummary()

  }


  /*****************************
  ****** Data Cleanance ********
  ******************************/
  def harmonizationCleanCampaignProduct(): DataFrame = {
    val campaign_products_raw: DataFrame =  utils.spark.read.table(amzDEraw + "campaign_products")
      .drop("status")
      .withColumnRenamed("enabled", "status")

    val campaign_products_one_title = campaign_products_raw.select("asin", "product_title")
      .distinct()
      .groupBy("asin")
      .agg(max("product_title").as("product_title"))

    val campaign_products_start_date: DataFrame = addDayMonthYearCols(campaign_products_raw, "start_date")
      .drop("product_title")
      .join(campaign_products_one_title, Seq("asin"), "inner")

    addTimeInfo(campaign_products_start_date, "start_date")

  }

  def harmonizationCleanCampaignsSummary(): Unit = {
    val campaign_summary_raw: DataFrame = utils.spark.read.table(amzDEraw + "campaigns_summary")

    val campaign_summary_start_date: DataFrame = campaign_summary_raw
      .filter(col("start_date_campaign").isNotNull)
      .withColumn("year", concat(lit("20"), substring(col("start_date_campaign").cast("string"),7 , 2)).cast("int"))
      .withColumn("month", substring(col("start_date_campaign").cast("string"),4 , 2).cast("int"))
      .withColumn("day", substring(col("start_date_campaign").cast("string"),1 , 2).cast("int"))
      .drop("start_date_campaign")
      .withColumn("start_date_campaign", concat(col("year"), lit("-"), col("month"), lit("-"), col("day")))
      .drop("year", "month", "day")

    addTimeInfo(addDayMonthYearCols(campaign_summary_start_date, "start_date"), "start_date")
      .drop("dummy_row")
      .filter((col("month")>7 && col("year") === 2018) || col ("year") =!= 2018)
      .withColumn("type_advertisement", when(col("type_advertisement") === "SB", lit("Sponsored Brands (SB)"))
        when(col("type_advertisement")==="SP", lit("Sponsored Products (SP)"))
        when(col("type_advertisement") === "HSA", lit("Sponsored Brands (SB)"))
        otherwise col("type_advertisement"))
      .writeHiveTable(amzDEhar + "campaigns_summary")

  }

  def harmonizationCleanCampaignsKeywords(): DataFrame = {
    val campaing_keywords_raw: DataFrame = utils.spark.read.table(amzDEraw + "campaign_keywords_hsa")
      .fullUnion(utils.spark.read.table(amzDEraw + "campaign_keywords_sp"))
      .drop(col("status"))
      .withColumnRenamed("enabled", "status")


    val campaign_keywords_start_date: DataFrame = addDayMonthYearCols(campaing_keywords_raw, "start_date")

    addTimeInfo(campaign_keywords_start_date, "start_date")
  }


  /*****************************
  ******* Tables Join **********
  ******************************/
  def harmonizationJoinProductSummary(): Unit = {
    val summary = utils.spark.read.table(amzDEhar + "campaigns_summary")
      .select("start_date", "campaign_name", "type_advertisement",
        "type_campaign", "budget","start_date_campaign")

    harmonizationCleanCampaignProduct()
      .join(summary
        , Seq("start_date", "campaign_name")
        , "inner"
      )
      .writeHiveTable(amzDEhar + "campaign_products")

  }

  def harmonizationJoinKeywordsSummary(): Unit = {
    val summary = utils.spark.read.table(amzDEhar + "campaigns_summary")
      .select("start_date", "campaign_name", "type_advertisement",
        "type_campaign", "budget", "start_date_campaign")

    harmonizationCleanCampaignsKeywords()
      .join(summary
        , Seq("start_date", "campaign_name")
        , "inner"
      ).writeHiveTable(amzDEhar + "campaign_keywords")

  }


  /*****************************
  ****** Commons methods *******
  *****************************/
  def addDayMonthYearCols(dataframe: DataFrame, colDates: String): DataFrame = {
    dataframe
      .filter(col(colDates).isNotNull)
      .withColumn("month", substring(col(colDates).cast("string"),6 , 2).cast("int"))
      .withColumn("year", substring(col(colDates).cast("string"),1 , 4).cast("int"))
      .withColumn("day", substring(col(colDates).cast("string"),9 , 2).cast("int"))
      .withColumn("month_str",utils.mappingMonthNameUDF(col("month")))
  }

  def addTimeInfo(dataframe: DataFrame, colDates: String): DataFrame = {
    dataframe
      .filter(col(colDates).isNotNull)
      .withColumn("week", weekofyear(col(colDates)).cast("int"))
      .withColumn("quarter", when(col("month") < 4 , 1)
        .when(col("month") > 3 and col("month")< 7, 2)
        .when(col("month") > 6 and col("month")< 10, 3)
        .otherwise(4))
  }

}
