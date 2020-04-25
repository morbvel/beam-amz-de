package com.beamsuntory.amz.de.harmonization

import com.beamsuntory.amz.de.commons.ModelsAmzDE
import com.beamsuntory.amz.de.harmonization.ams.HarmonizationAms
import com.beamsuntory.amz.de.functional.ams.FunctionalAmsEdw
import com.beamsuntory.amz.de.test.FeatureBasicTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.Assert
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HarmonizationAmsTest  extends FeatureBasicTest {

  test("Test the Arap Harmonization Flow"){

      Given("Sales diagnostic and geographic tables valid from Arap")

      val ut = utils.get
      ut.createTableWithCaseClass("campaign_keywords_hsa", "src/test/resources/campaign_keywords_hsa.csv", ";", "", ModelsAmzDE, "true")
      ut.createTableWithCaseClass("campaign_keywords_sp", "src/test/resources/campaign_keywords_sp.csv", ";", "", ModelsAmzDE, "true")
      ut.createTableWithCaseClass("campaign_products", "src/test/resources/campaign_products.csv", ";", "", ModelsAmzDE, "true")
      ut.createTableWithCaseClass("campaigns_summary", "src/test/resources/campaigns_summary.csv", ";", "", ModelsAmzDE, "true")

      val harm = new HarmonizationAms(ut)
      val func = new FunctionalAmsEdw(ut)

      /*harm.harmonizationCleanCampaignsSummary()
      harm.harmonizationJoinProductSummary()
      harm.harmonizationJoinKeywordsSummary()

      func.createScenarios()*/

  }
}