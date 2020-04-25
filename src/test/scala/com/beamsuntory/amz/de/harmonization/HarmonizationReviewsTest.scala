package com.beamsuntory.amz.de.harmonization

import com.beamsuntory.amz.de.commons.ModelsAmzDE
import com.beamsuntory.amz.de.harmonization.reviews.HarmonizationReviews
import com.beamsuntory.amz.de.test.FeatureBasicTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HarmonizationReviewsTest extends FeatureBasicTest with Serializable {

  test("Test for reviews analysis"){
    /*Given("Sales diagnostic and geographic tables valid from Arap")

    val ut = utils.get
    ut.createTableWithCaseClass("sentiment_analysis", "src/test/resources/sentiments.csv", ";", "", ModelsAmzDE, "true")
    ut.createTableWithCaseClass("amz_review_products", "src/test/resources/amz_review_products.csv", ";", "", ModelsAmzDE, "true")
    ut.createTableWithCaseClass("asin_product_hierarchy", "src/test/resources/asin_hierarchy.csv", ";", "", ModelsAmzDE, "true")

    val harm = new HarmonizationReviews(ut)

    println("Hey")
    harm.mainExecution()*/
  }
}