package com.beamsuntory.amz.de.harmonization

import com.beamsuntory.amz.de.commons.ModelsAmzDE
import com.beamsuntory.amz.de.functional.arap.FunctionalArapEdw
import com.beamsuntory.amz.de.harmonization.arap.HarmonizationArap
import com.beamsuntory.amz.de.test.FeatureBasicTest
import com.beamsuntory.bgc.commons.DataFrameUtils.{amzDEhar, amzDEraw}
import com.beamsuntory.thebar.gl.commons.ModelsEDW
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.Assert

@RunWith(classOf[JUnitRunner])
class HarmonizationArapTest  extends FeatureBasicTest {

  test("Test the Arap Harmonization Flow"){


      Given("Sales diagnostic and geographic tables valid from Arap")

      val ut = utils.get
      ut.createTableWithCaseClass("sales_diagnostic_ordered", "src/test/resources/sales_diagnostic_ordered.csv", ";", "", ModelsAmzDE, "true")
      ut.createTableWithCaseClass("sales_diagnostic_shipped", "src/test/resources/sales_diagnostic_shipped.csv", ";", "", ModelsAmzDE, "true")
      ut.createTableWithCaseClass("sales_diagnostic_shipped_sourcing", "src/test/resources/sales_diagnostic_sourcing.csv", ";", "", ModelsAmzDE, "true")
      ut.createTableWithCaseClass("geographic_sales_insights", "src/test/resources/geographic_sales.csv", ";", "", ModelsAmzDE, "true")
      ut.createTableWithCaseClass("germany_cities", "src/test/resources/germany_cities.csv", ",", "", ModelsAmzDE, "true")
      ut.createTableWithCaseClass("product_catalog", "src/test/resources/product_catalog.csv", ";", "", ModelsAmzDE, "true")

      val harm = new HarmonizationArap(ut)

      When("Try to execute Sales Diagnostic Intermediate Tables")

      harm.harmonizationCleanSalesOrdered()
      harm.harmonizationCleanSalesShipped()
      harm.harmonizationCleanSalesShippedSourcing()

      val sales_diagnostic_ordered_stg: DataFrame = ut.spark.read.table("sales_diagnostic_ordered_stg")
      val sales_diagnostic_shipped_stg: DataFrame = ut.spark.read.table("sales_diagnostic_shipped_stg")
      val sales_diagnostic_shipped_sourcing_stg: DataFrame = ut.spark.read.table("sales_diagnostic_shipped_sourcing_stg")

      Then("The results of the sales diagnostic ordered stg")

      val fields_sales_diagnostic_ordered_stg :Row = sales_diagnostic_ordered_stg
        .filter(col("asin") === "B0094QG1EK" and col("year") === 2018)
        .select("ordered_revenue", "year", "ordered_units_inc_last_year")
        .collect()(0)

      Assert.assertEquals("Ordered revenue must be", 1020.48,
        fields_sales_diagnostic_ordered_stg.getAs("ordered_revenue").asInstanceOf[Double], 0.01)
      Assert.assertEquals("Ordered units inc last year must be", 2618.0,
        fields_sales_diagnostic_ordered_stg.getAs("ordered_units_inc_last_year").asInstanceOf[Double], 0.01)
      Assert.assertEquals("Year must be", 2018,
        fields_sales_diagnostic_ordered_stg.getAs("year").asInstanceOf[Integer])

      Then("The results of the sales diagnostic shipped stg")

      val fields_sales_diagnostic_shipped_stg :Row = sales_diagnostic_shipped_stg
        .filter(col("asin") === "B001HUA3FY" and col("year") === 2018)
        .select("shipped_revenue", "year", "shipped_revenue_last_year")
        .collect()(0)

      Assert.assertEquals("Shipped revenue must be", 469.28,
        fields_sales_diagnostic_shipped_stg.getAs("shipped_revenue").asInstanceOf[Double], 0.01)
      Assert.assertEquals("Shipped revenue last year must be", 188.0,
        fields_sales_diagnostic_shipped_stg.getAs("shipped_revenue_last_year").asInstanceOf[Double], 0.01)
      Assert.assertEquals("Year must be", 2018,
        fields_sales_diagnostic_shipped_stg.getAs("year").asInstanceOf[Integer])

      Then("The results of the sales diagnostic shipped sourcing stg")

      val fields_sales_diagnostic_shipped_sourcing_stg :Row = sales_diagnostic_shipped_sourcing_stg
        .filter(col("asin") === "B001HUA3FY" and col("year") === 2018)
        .select("shipped_revenue_sourcing", "year", "shipped_revenue_sourcing_last_year")
        .collect()(0)

      Assert.assertEquals("Shipped revenue sourcing must be", 55555.0,
        fields_sales_diagnostic_shipped_sourcing_stg.getAs("shipped_revenue_sourcing").asInstanceOf[Double], 0.01)
      Assert.assertEquals("Shipped revenue sourcing last year must be", 66666.0,
        fields_sales_diagnostic_shipped_sourcing_stg.getAs("shipped_revenue_sourcing_last_year").asInstanceOf[Double], 0.01)
      Assert.assertEquals("Year must be", 2018,
        fields_sales_diagnostic_shipped_sourcing_stg.getAs("year").asInstanceOf[Integer])

      When("Try to execute Geographic Sales Cleaning Exectution")

      harm.harmonizationCleanGeographicSales()

      Then("The result is the geographic sales cleaning table")

      val geographic_sales = ut.spark.read.table("geographic_sales_insights_stg")

      Assert.assertEquals("Number of rows in Geographic Sales Insights", 6, geographic_sales.count)

      val fields_geographic_sales :Row = geographic_sales
        .filter(col("asin") === "B003ZIRTM6")
        .select("shipped_revenue", "latitude", "longitude", "brand", "state")
        .collect()(0)

      Assert.assertEquals("Shipped revenue must be", 197.47,
        fields_geographic_sales.getAs("shipped_revenue").asInstanceOf[Double], 0.01)
      Assert.assertEquals("Latitude must be", 53.0618,
        fields_geographic_sales.getAs("latitude").asInstanceOf[Double], 0.01)
      Assert.assertEquals("Longitude must be", 8.79,
        fields_geographic_sales.getAs("longitude").asInstanceOf[Double], 0.01)
      Assert.assertEquals("Brand must be", "Bowmore",
        fields_geographic_sales.getAs("brand"))
      Assert.assertEquals("State must be", "Bremen",
        fields_geographic_sales.getAs("state"))


  }
}