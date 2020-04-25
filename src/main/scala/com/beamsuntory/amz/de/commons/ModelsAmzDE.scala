package com.beamsuntory.amz.de.commons

import com.beamsuntory.amz.de.commons.models.ModelsCaseClass
import com.beamsuntory.bgc.commons.Models
import org.apache.spark.sql.{DataFrame, DataFrameReader}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

/**
  * Object (static) that holds the tables Case Classes and mappings.
  */

object ModelsAmzDE extends Models with ModelsCaseClass {

  org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)



  /**
    * Function that match a specific table name to its corresponding Case Class, converting its DataFrame to
    * DataSet accordingly.
    *
    * @param table: name of the DataFrame table.
    * @param dataFrame: DataFrame corresponding to the table.
    * @return DataSet built from the corresponding Case Class for the table.
    */
  // scalastyle:off
  override def toDataFrame(table: String, dataFrame: DataFrameReader, path: String): DataFrame  = {
    table match {

      case "sales_diagnostic_shipped" => dataFrame.schema(ScalaReflection.schemaFor[SalesDiagnosticShipped].dataType.asInstanceOf[StructType]).load(path)
      case "sales_diagnostic_shipped_sourcing" => dataFrame.schema(ScalaReflection.schemaFor[SalesDiagnosticShippedSourcing].dataType.asInstanceOf[StructType]).load(path)
      case "sales_diagnostic_ordered" => dataFrame.schema(ScalaReflection.schemaFor[SalesDiagnosticOrdered].dataType.asInstanceOf[StructType]).load(path)
      case "germany_cities" => dataFrame.schema(ScalaReflection.schemaFor[GermanyCities].dataType.asInstanceOf[StructType]).load(path)
      case "geographic_sales_insights" => dataFrame.schema(ScalaReflection.schemaFor[GeographicSalesInsights].dataType.asInstanceOf[StructType]).load(path)
      case "alternate_purchase" => dataFrame.schema(ScalaReflection.schemaFor[AlternatePurchase].dataType.asInstanceOf[StructType]).load(path)
      case "amz_terms" => dataFrame.schema(ScalaReflection.schemaFor[AmzTerms].dataType.asInstanceOf[StructType]).load(path)
      case "customer_reviews" => dataFrame.schema(ScalaReflection.schemaFor[CustomerReviews].dataType.asInstanceOf[StructType]).load(path)
      case "forecast_inventory_ordered" => dataFrame.schema(ScalaReflection.schemaFor[ForecastInventoryOrdered].dataType.asInstanceOf[StructType]).load(path)
      case "forecast_inventory_shipped" => dataFrame.schema(ScalaReflection.schemaFor[ForecastInventoryShipped].dataType.asInstanceOf[StructType]).load(path)
      case "inventory_health" => dataFrame.schema(ScalaReflection.schemaFor[InventoryHealth].dataType.asInstanceOf[StructType]).load(path)
      case "item_comparison" => dataFrame.schema(ScalaReflection.schemaFor[ItemComparison].dataType.asInstanceOf[StructType]).load(path)
      case "market_basket_analysis" => dataFrame.schema(ScalaReflection.schemaFor[MarketBasketAnalysis].dataType.asInstanceOf[StructType]).load(path)
      case "campaign_products" => dataFrame.schema(ScalaReflection.schemaFor[campaignProducts].dataType.asInstanceOf[StructType]).load(path)
      case "campaigns_summary" => dataFrame.schema(ScalaReflection.schemaFor[campaignsSummary].dataType.asInstanceOf[StructType]).load(path)
      case "campaign_keywords" => dataFrame.schema(ScalaReflection.schemaFor[campaignKeywords].dataType.asInstanceOf[StructType]).load(path)
      case "material_amazon" => dataFrame.schema(ScalaReflection.schemaFor[materialAmazon].dataType.asInstanceOf[StructType]).load(path)
      case "product_catalog" => dataFrame.schema(ScalaReflection.schemaFor[productCatalog].dataType.asInstanceOf[StructType]).load(path)
      case "asin_product_hierarchy" => dataFrame.schema(ScalaReflection.schemaFor[asinHierarchy].dataType.asInstanceOf[StructType]).load(path)
      case "edw_material" => dataFrame.schema(ScalaReflection.schemaFor[edwMaterial].dataType.asInstanceOf[StructType]).load(path)
      case "campaigns_summary" => dataFrame.schema(ScalaReflection.schemaFor[campaignsSummary].dataType.asInstanceOf[StructType]).load(path)
      case "campaign_keywords_hsa" => dataFrame.schema(ScalaReflection.schemaFor[campaignKeywords].dataType.asInstanceOf[StructType]).load(path)
      case "campaign_keywords_sp" => dataFrame.schema(ScalaReflection.schemaFor[campaignKeywords].dataType.asInstanceOf[StructType]).load(path)
      case "sentiment_analysis" => dataFrame.schema(ScalaReflection.schemaFor[sentimentAnalysis].dataType.asInstanceOf[StructType]).load(path)
      case "amz_review_products" => dataFrame.schema(ScalaReflection.schemaFor[reviewProducts].dataType.asInstanceOf[StructType]).load(path)
    }
  }
  // scalastyle:on

}

