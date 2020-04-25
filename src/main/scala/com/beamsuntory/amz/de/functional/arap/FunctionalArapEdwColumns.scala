package com.beamsuntory.amz.de.functional.arap

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

trait FunctionalArapEdwColumns {


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
    col("material_description"),
    col("size_1"),
    col("size"),
    col("zrptmb").as("brand_partner_code"),
    col("zrptmb_description").as("brand_partner")
  )

  val selMaterialHierarchyColumns: Seq[Column] = Seq(
    col("material").as("b_material"),
    col("materialdescription").as("b_material_description"),
    col("materialtype").as("b_material_type"),
    col("size").as("b_size"),
    col("bottlepercase").as("b_bottle_per_case"),
    col("prod_h1").as("b_prod_h1"),
    col("prod_h1_txt").as("b_prod_h1_txt"),
    col("prod_h2").as("b_prod_h2"),
    col("prod_h2_txt").as("b_prod_h2_txt"),
    col("prod_h3").as("b_prod_h3"),
    col("prod_h3_txt").as("b_prod_h3_txt"),
    col("prod_h4").as("b_prod_h4"),
    col("prod_h4_txt").as("b_prod_h4_txt")
  )

  val selSalesColumns: Seq[Column] = Seq(
    col("asin"),
    col("start_date"),
    col("ordered_shipped_units_conversion_rate"),
    col("ordered_shipped_revenue_conversion_rate")
  )
}
