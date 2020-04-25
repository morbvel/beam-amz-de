package com.beamsuntory.amz.de.reporting

import com.beamsuntory.amz.de.commons.ModelsAmzDE
import com.beamsuntory.amz.de.functional.arap.FunctionalArapEdw
import com.beamsuntory.amz.de.reporting.arap.ReportingArap
import com.beamsuntory.amz.de.test.FeatureBasicTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.Assert
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReportingArapTest extends FeatureBasicTest {

  test("Test the Arap Harmonization Flow"){

    /* scenario("Normal Flow") {
      Given("2 tables for Reporting Arap")

      val ut = utils.get

      utils.get.spark.read.parquet("src/test/resources/harmonization/geographic_sales_insights")
        .createTempView("geographic_sales_insights_stg")
      utils.get.spark.read.parquet("src/test/resources/harmonization/sales_diagnostic")
        .createTempView("sales_diagnostic")
      ut.createTableWithCaseClass("edw_material", "src/test/resources/cv_h_at_material", ";", "", ModelsAmzDE, "true")
      ut.createTableWithCaseClass("material_amazon", "src/test/resources/cv_h_at_material_unit_of_measure", ";", "", ModelsAmzDE, "true")
      ut.createTableWithCaseClass("asin_product_hierarchy", "src/test/resources/asin_hierarchy.csv", ";", "", ModelsAmzDE, "true")


      val func = new FunctionalArapEdw(ut)
      func.mainExecution()
      val rep = new ReportingArap(ut)
      rep.mainExecution()

      Then("The results are the geographic and sales tables")

      val sales_diagnostic:DataFrame = ut.spark.read.table("sales_diagnostic")

      Assert.assertEquals("Number of rows in Sales diagnostic", 12, sales_diagnostic.count)

      val geographic_sales = ut.spark.read.table("geographic_sales_insights")
      Assert.assertEquals("Number of rows in Geographic Sales Insights", 6, geographic_sales.count)

    } */

  }

}