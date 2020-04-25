package com.beamsuntory.amz.de.harmonization.reviews

import com.beamsuntory.amz.de.commons.UtilsAmzDE
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

trait HarmonizationReviewsColumns {
  val utils: UtilsAmzDE

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

  val selectReviews: Seq[Column] = Seq(
    col("year"),
    concat_ws("-",trim(col("month")),trim(col("day")),trim(col("year"))).as("date"),
    when(substring(col("helpful_vote"), 0, 1) > 0, substring(col("helpful_vote"), 0, 1) )
      .otherwise( when(length(col("helpful_vote")) > 0, lit(1)).otherwise(lit(0)) )
        .as("likes"),
    substring(col("rating"), 0, 1).as("_rating"),
    when(col("verified_buy").equalTo("null"), lit(0))
      .otherwise(lit(1))
        .as("_verified_buy"),
    col("review_body"),
    col("asin"),
    col("product_title").as("title"),
    col("review_title"),
    col("month"),
    col("day"),
    col("id_review")
  )

}
