package com.beamsuntory.amz.de.functional.reviews

import com.beamsuntory.amz.de.commons.UtilsAmzDE
import com.beamsuntory.bgc.commons.DataFrameUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


class FunctionalReviews(val utils: UtilsAmzDE) {


  def mainExecution(): Unit = {
    //simpleFunctionalLayer
    reviewsSentiments
  }

  def reviewsSentiments(): Unit = {

    val revs: DataFrame = utils.spark.read.table(amzDEraw + "reviews_hierarchy")
    val analysis: DataFrame = utils.spark.read.table(amzDEraw + "sentiment_analysis")

    revs.join(analysis, Seq("id_review"), "left_outer").distinct().writeHiveTable(amzDEfun + "reviews_hierarchy")

  }

  def simpleFunctionalLayer(): Unit = {
    utils.spark.read.table(amzDEhar + "reviews_hierarchy").writeHiveTable(amzDEfun + "reviews_hierarchy")
  }

}
