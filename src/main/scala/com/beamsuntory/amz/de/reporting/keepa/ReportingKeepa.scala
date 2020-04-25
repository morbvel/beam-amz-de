package com.beamsuntory.amz.de.reporting.keepa

import java.util.Calendar

import com.beamsuntory.amz.de.commons.UtilsAmzDE
import com.beamsuntory.bgc.commons.DataFrameUtils._
import org.apache.spark.sql.functions._


class ReportingKeepa(val utils: UtilsAmzDE){


  def mainExecution(): Unit = {

    reportingKeepa
  }

  def reportingKeepa(): Unit = {

    utils.spark.read.table(amzDEfun + "keepa_pricing_hierarchy")
      .writeHiveTable(amzDErep + "keepa_pricing_hierarchy")

    utils.spark.read.table(amzDEfun + "geographic_sales_keepa")
      .writeHiveTable(amzDErep + "geographic_sales_keepa")
  }

}