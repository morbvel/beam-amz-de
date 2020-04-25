package com.beamsuntory.amz.de.harmonization.keepa

import com.beamsuntory.amz.de.commons.UtilsAmzDE
import com.beamsuntory.bgc.commons.DataFrameUtils._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}


class HarmonizationHierarchy(val utils: UtilsAmzDE) {


  val selMaterialColumns: Seq[Column] = Seq(
    col("material"),
    col("material_description"),
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
    col("price_class"),
    col("bpc_prod"),
    col("changedon"),
    col("createdon"),
    col("material_description"),
    col("size_1"),
    col("size"),
    col("zrptmb").as("brand_partner_code"),
    col("zrptmb_description").as("brand_partner")
  )

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
    col("brand_partner"),
    col("createdon"),
    col("changedon")
  )

  def mainExecution(): Unit = {
    harmonizationHierarchy
  }

  def harmonizationHierarchy(): Unit = {
    val asin_ean_upc: DataFrame = utils.spark.read.table(amzDEraw + "keepa_asin_eanupc")
      .filter(col("eanupc").isNotNull)
      .withColumn("eanupc", col("eanupc").cast("bigint"))
      .distinct()

    val product_catalog: DataFrame = utils.spark.table(amzDEraw + "product_catalog")
      .select(col("asin"), col("model_number"))

    val edw_material: DataFrame =  utils.spark.read.table(amzDEraw + "edw_material").select(selMaterialColumns: _*)
      .withColumn("changedon", regexp_replace(col("changedon"), "\\.", "-"))
      .withColumn("createdon", regexp_replace(col("createdon"), "\\.", "-"))


    val material_amazon: DataFrame = utils.spark.read.table(amzDEraw + "material_amazon")
      .filter(col("eanupc").isNotNull)
      .select( col("eanupc").cast("bigint"), col("material"))

    val asin_material_catalog: DataFrame = product_catalog.withColumnRenamed("model_number", "material")
      .join(material_amazon, Seq("material"), "inner")
      .fullUnion(
        product_catalog.withColumnRenamed("model_number", "eanupc")
          .join(material_amazon, Seq("eanupc"), "inner")
      )

    val ean_material_all: DataFrame = asin_material_catalog.select("asin", "eanupc")
      .join(material_amazon, Seq("eanupc"), "inner")

    val all_material_join: DataFrame = asin_ean_upc.join(material_amazon, Seq("eanupc"), "left_outer")
      .fullUnion(asin_material_catalog)
      .fullUnion(ean_material_all)
      .distinct()
      .join(edw_material, Seq("material"), "left_outer")
      .drop("material_description")

    all_material_join.writeHiveTable(amzDEhar + "material_asin_amz_de")

    val all_possible_hierarchy = cleaningHierarchy(all_material_join)

    val asin_product_hierarchy: DataFrame = utils.spark.read.table(amzDEraw + "asin_product_hierarchy")
        .filter(col("prodh1").isin("S2", "W2", "DELETED"))

      all_possible_hierarchy
      .fullUnion(asin_product_hierarchy)
        .filter(col("brand_partner") =!= "" or col("brand_partner").isNotNull)
        .writeHiveTable(amzDEhar + "hierarchy_asin_amz_de")
  }

  def cleaningHierarchy(all_material_join: DataFrame): DataFrame = {

    val windowlastesHierarchyChanged = Window.partitionBy("asin").orderBy(col("changedon").desc)
    val windowlastesHierarchyCreated = Window.partitionBy("asin").orderBy(col("createdon").desc)

    all_material_join
        .select(selHierarchyColumns: _*)
      .distinct()
      .filter(col("brand_partner") =!= "")
      .filter(col("brand_partner") =!= "not applicable")
      .withColumn("most_recent_changedon", first("changedon").over(windowlastesHierarchyChanged))
      .filter(col("most_recent_changedon") === col("changedon"))
      .withColumn("most_recent_createdon", first("createdon").over(windowlastesHierarchyChanged))
      .filter(col("most_recent_createdon") === col("createdon"))
      .filter(col("prodh3_description").isNotNull)
      .drop("createdon", "changedon", "most_recent_changedon", "most_recent_createdon")
      .groupBy("asin", "prodh1", "prodh1_description", "prodh2", "prodh2_description", "prodh3", "prodh3_description",
      "prodh4", "prodh4_description", "prodh5", "prodh5_description", "brand_partner")
        .agg(min("prodh6").as("prodh6"),
        min("prodh6_description").as("prodh6_description"),
        min("size").as("size"))
      .withColumn("brand_partner", upper(col("brand_partner")))
      .filter(col("asin") =!= "asin")

  }


  def functionalHierarchy2(): Unit = {

    val asin_ean_upc: DataFrame = utils.spark.read.table(amzDEraw + "keepa_asin_eanupc")
      .filter(col("eanupc").isNotNull)
      .withColumn("eanupc", col("eanupc").cast("bigint"))
      .distinct()

    val product_catalog: DataFrame = utils.spark.table(amzDEraw + "product_catalog")
      .select(col("asin"), col("model_number"))

    val edw_material: DataFrame =  utils.spark.read.table(amzDEraw + "edw_material").select(selMaterialColumns: _*)
      .withColumn("changedon", regexp_replace(col("changedon"), "\\.", "-"))

    val material_amazon: DataFrame = utils.spark.read.table(amzDEraw + "material_amazon")
      .filter(col("eanupc").isNotNull)
      .select( col("eanupc").cast("bigint"), col("material"))

    val asin_material_catalog: DataFrame = product_catalog.withColumnRenamed("model_number", "material")
      .join(material_amazon, Seq("material"), "inner")
      .fullUnion(
      product_catalog.withColumnRenamed("model_number", "eanupc")
          .join(material_amazon, Seq("eanupc"), "inner")
      )

    val ean_material_all: DataFrame = asin_material_catalog.select("asin", "eanupc")
      .join(material_amazon, Seq("eanupc"), "inner")

    val all_material_join: DataFrame = asin_ean_upc.join(material_amazon, Seq("eanupc"), "left_outer")
      .fullUnion(asin_material_catalog)
      .fullUnion(ean_material_all)
      .distinct()
      .join(edw_material, Seq("material"), "left_outer")
      .drop("material_description")
      //.filter(col("material").isNotNull)

    val windowlastesHierarchy = Window.partitionBy("asin").orderBy(col("changedon").desc)

    val shipment_calculated = utils.spark.read.table(amzDEhar + "shipment_amz_de")
      .drop("eanupc")
      .withColumnRenamed("material_edw", "material")

    shipment_calculated.join(all_material_join.select("asin", "material"), Seq("material"), "left_outer")
      .writeHiveTable(amzDEfun + "tmp_shipment")

    all_material_join
      .withColumn("most_recent_changedon", first("changedon").over(windowlastesHierarchy))
      //.filter(col("most_recent_changedon") === col("changedon"))
      .distinct()
      .drop("material_description")
      .writeHiveTable(amzDEfun + "tmp_hierarchy")

    all_material_join.writeHiveTable(amzDEfun + "tmp_all_materials")

  }

}
