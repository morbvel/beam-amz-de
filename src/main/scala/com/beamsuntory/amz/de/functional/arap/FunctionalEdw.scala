package com.beamsuntory.amz.de.functional.arap

import com.beamsuntory.amz.de.commons.UtilsAmzDE
import com.beamsuntory.bgc.commons.DataFrameUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._


class FunctionalEdw(val utils: UtilsAmzDE) {


  def mainExecution(): Unit = {
    //functionalShipment
  }


  def functionalShipment(): Unit = {

    val shipment_calculated = utils.spark.read.table(amzDEhar + "shipment_amz_de")
      .drop("eanupc")
      .withColumnRenamed("material_edw", "material")

    val material_amazon: DataFrame = utils.spark.read.table(amzDEraw + "material_amazon")
      .filter(col("eanupc").isNotNull)
      .filter( col("alternative_unit").equalTo("BOT") )
      .select( col("eanupc"), col("material"))
      .withColumn("eanupc", col("eanupc").cast("String"))
      .withColumnRenamed("eanupc", "ean")

    val sales_diagnostic = utils.spark.read.table(amzDEfun + "sales_diagnostic")
      .select("product_title", "ean", "asin")
      .dropDuplicates()

    val material_amz = shipment_calculated.join(material_amazon, Seq("material"), "left_outer")
    material_amz.writeHiveTable(amzDEfun + "shipments_material_amz")

    material_amz
      .join(sales_diagnostic, Seq("ean"), "left_outer")
      .writeHiveTable(amzDEfun + "shipments_sales_amz_de")
  }

}
