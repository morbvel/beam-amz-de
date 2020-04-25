package com.beamsuntory.amz.de.reporting.reviews

import com.beamsuntory.amz.de.commons.UtilsAmzDE
import com.beamsuntory.bgc.commons.DataFrameUtils._


class ReportingReviews(val utils: UtilsAmzDE) {


  def mainExecution(): Unit = {
    simpleReportingLayer
  }

  def simpleReportingLayer(): Unit = {

    utils.spark.read.table(amzDEfun + "reviews_hierarchy").writeHiveTable(amzDErep + "reviews_hierarchy")

  }

}
