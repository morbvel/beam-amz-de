package com.beamsuntory.amz.de.harmonization.arap

import com.beamsuntory.amz.de.commons.UtilsAmzDE
import com.beamsuntory.bgc.commons.DataFrameUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._


class HarmonizationEdw(val utils: UtilsAmzDE) extends HarmonizationEdwColumns{

  val shipment_customer_sales: List[String] = utils.config.getStringList("shipment_customer_sales").toList
  val shipment_detail_germany_detail : List[String] = utils.config.getStringList("shipment_detail_germany_details").toList
  val shipment_detail_germany_versions: List[String] = utils.config.getStringList("shipment_detail_germany_versions").toList


  def mainExecution(): Unit = {
    harmonizationShipments()
  }


  def harmonizationShipments(): Unit = {

    val edw_shipments_stg: DataFrame = utils.spark.read.table(TheBarGLraw + "edw_shipments")
      .filter(col("company_code") =!= "Company Code")
      .withColumnRenamed("sales_organization","salesorg")
      .withColumnRenamed("distribution_channel","distr_chan")
    val raw_edw_shipments: DataFrame = utils.coalescenceGLColumns(edw_shipments_stg, edw_shipments_stg.columns: _*)
    val raw_edw_material: DataFrame = utils.spark.read.table(amzDEraw + "edw_material").select(selMaterialColumns: _*)
    val raw_edw_customer: DataFrame = utils.loadGLCustomer(utils.spark.read.table(TheBarGLraw + "edw_customer"))
      .select(selCustomerColumns: _*)
      .filter(col("cust_sales").isin(shipment_customer_sales: _*))

    raw_edw_shipments
      .join(raw_edw_customer, Seq("cust_sales", "salesorg", "distr_chan", "division"), "inner")
      .join(raw_edw_material, Seq("material_edw"), "inner")
      .filter(col("version").isin(shipment_detail_germany_versions: _*))
      .filter(col("detail").isin(shipment_detail_germany_detail: _*))
      .cache()
      .withColumn("value", col("value").cast("double"))
      .withColumn("volume", col("volume").cast("double"))
      .drop("zzcusth5", "zzcusth6", "unit")
      .groupBy("year","month","salesorg","salesorg_description","distr_chan","division","sales_dist", "sales_dist_description",
        "country","country_description","zzcusth2_description","zzcusth3_description",
        "zzcusth5_description","zzcusth6_description","zzcusth7","zzcusth7_description","cust_sales","cust_sales_description",
        "material_edw","material_description","size_1","bottle_per_case","size", "prodh1", "prodh1_description","prodh2",
        "prodh2_description","prodh3","prodh3_description","prodh4","prodh4_description","prodh5", "prodh5_description",
        "prodh6","prodh6_description","price_class","bpc_prod","brand_partner_code", "brand_partner")
      .agg(
        sum(when(col("version") === lit("000") && col("detail") === lit("Gross Profit"), col("value"))
          .otherwise(lit(null))).as("gross_profit_actual"),
        sum(when(col("version") === lit("010") && col("detail") === lit("Gross Profit"), col("value"))
          .otherwise(lit(null))).as("gross_profit_budget"),
        sum(when(col("version") === lit("000") && col("detail") === lit("Net Sales w/o FET"), col("value"))
          .otherwise(lit(null))).as("net_sales_withoutfet_actual"),
        sum(when(col("version") === lit("010") && col("detail") === lit("Net Sales w/o FET"), col("value"))
          .otherwise(lit(null))).as("net_sales_withoutfet_budget"),
        sum(when(col("version") === lit("000") && col("detail") === lit("Volume 9L"), col("volume"))
          .otherwise(lit(null))).as("volume_9l_actual"),
        sum(when(col("version") === lit("010") && col("detail") === lit("Volume 9L"), col("volume"))
          .otherwise(lit(null))).as("volume_9l_budget"),
        sum(when(col("version") === lit("000") && col("detail") === lit("Brand Investment"), col("value"))
          .otherwise(lit(null))).as("brand_investment_actual"),
        sum(when(col("version") === lit("010") && col("detail") === lit("Brand Investment"), col("value"))
          .otherwise(lit(null))).as("brand_investment_budget"))
      .drop("detail","version")
      .writeHiveTable(amzDEhar + "shipment_amz_de")
  }

}
