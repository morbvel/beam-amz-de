package com.beamsuntory.amz.de.reporting.ams

import com.beamsuntory.amz.de.commons.UtilsAmzDE
import com.beamsuntory.bgc.commons.DataFrameUtils._
import org.apache.spark.sql.functions.lit


class ReportingAms(val utils: UtilsAmzDE){


  def mainExecution(): Unit = {

    reportingMarketing
    reportingScenarios
  }

  def reportingMarketing(): Unit = {
    val radar_table = utils.spark.read.table(amzDEfun + "campaigns_summary_radar")
      .withColumn("keywords_products", lit("radar"))
    val product_keyword_campaign = utils.spark.read.table(amzDEfun + "product_keyword_campaign_old")

    radar_table.fullUnion(product_keyword_campaign).writeHiveTable(amzDErep + "product_keyword_campaign")

  }

  def reportingScenarios(): Unit = {
    utils.spark.read.table(amzDEfun + "campaign_analytics").writeHiveTable(amzDErep + "campaign_analytics")
  }

}

