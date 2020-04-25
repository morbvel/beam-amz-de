package com.beamsuntory.amz.de.functional

import com.beamsuntory.amz.de.commons.ModelsAmzDE
import com.beamsuntory.amz.de.functional.arap.FunctionalArapEdw
import com.beamsuntory.amz.de.test.FeatureBasicTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.Assert
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FunctionalArapTest extends FeatureBasicTest {

  test("Test the Arap Harmonization Flow"){

    /*scenario("Normal Flow") {
      Given("3 tables for functional Arap")

      val ut = utils.get

      utils.get.spark.read.parquet("src/test/resources/harmonization/geographic_sales_insights")
        .createTempView("geographic_sales_insights_stg")
      utils.get.spark.read.parquet("src/test/resources/harmonization/sales_diagnostic")
        .createTempView("sales_diagnostic")
      ut.createTableWithCaseClass("edw_material", "src/test/resources/cv_h_at_material", ";", "", ModelsAmzDE, "true")
      ut.createTableWithCaseClass("material_amazon", "src/test/resources/cv_h_at_material_unit_of_measure", ";", "", ModelsAmzDE, "true")
      ut.createTableWithCaseClass("asin_product_hierarchy", "src/test/resources/asin_hierarchy.csv", ";", "", ModelsAmzDE, "true")


      val func = new FunctionalArapEdw(ut)

      Then("The result is the hierarchy table")

      func.createMaterialTableWithHierarchy()
      val edw_material_hierarchy_amazon: DataFrame = ut.spark.read.table("edw_material_hierarchy_amazon")

      val fields_edw_material_hierarchy_amazon :Row = edw_material_hierarchy_amazon
        .filter(col("material") === "PN600436" )
        .select("brand_partner", "prodh6_description", "size_1")
        .collect()(0)

      Assert.assertEquals("Brand Partner must be", "BEAM",
        fields_edw_material_hierarchy_amazon.getAs("brand_partner"))
      Assert.assertEquals("Prodh6 description must be", "JIM BEAM WHITE",
        fields_edw_material_hierarchy_amazon.getAs("prodh6_description"))
      Assert.assertEquals("Size must be", "1500",
        fields_edw_material_hierarchy_amazon.getAs("size_1"))

      func.functionalEdw()

      Then("The results are the sales diagnostic and geographic table")

      val sales_diagnostic:DataFrame = ut.spark.read.table("sales_diagnostic")

      Assert.assertEquals("Number of rows in Sales diagnostic", 12, sales_diagnostic.count)

      val fields_sales_diagnostic :Row = sales_diagnostic
        .filter(col("asin") === "B004EY1ZCK" and col("year") === 2018)
        .select("ordered_revenue", "shipped_revenue", "prodh5_description")
        .collect()(1)

      Assert.assertEquals("Ordered revenue must be", 383.04,
        fields_sales_diagnostic.getAs("ordered_revenue").asInstanceOf[Double], 0.01)
      Assert.assertEquals("Prodh5 description must be", "JIM BEAM WHITE",
        fields_sales_diagnostic.getAs("prodh5_description"))
      Assert.assertEquals("Shipped revenue must be", 734.16,
        fields_sales_diagnostic.getAs("shipped_revenue").asInstanceOf[Double], 0.01)

      val geographic_sales = ut.spark.read.table("geographic_sales_insights")

      Assert.assertEquals("Number of rows in Geographic Sales Insights", 6, geographic_sales.count)

      val fields_geographic_sales :Row = geographic_sales
        .filter(col("asin") === "B003ZINBLE")
        .select("shipped_revenue", "latitude", "longitude", "material", "prodh2_description")
        .collect()(0)

      Assert.assertEquals("Shipped revenue must be", 96.6,
        fields_geographic_sales.getAs("shipped_revenue").asInstanceOf[Double], 0.01)
      Assert.assertEquals("Latitude must be", 52.5833,
        fields_geographic_sales.getAs("latitude").asInstanceOf[Double], 0.01)
      Assert.assertEquals("Longitude must be", 13.3167,
        fields_geographic_sales.getAs("longitude").asInstanceOf[Double], 0.01)
      Assert.assertEquals("Material must be", "DE000018",
        fields_geographic_sales.getAs("material"))
      Assert.assertEquals("Prodh2 description must be", "BOURBON",
        fields_geographic_sales.getAs("prodh2_description"))
    }*/

  }

}