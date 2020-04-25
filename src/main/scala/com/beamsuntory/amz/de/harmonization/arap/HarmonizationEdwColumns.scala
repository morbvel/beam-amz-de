package com.beamsuntory.amz.de.harmonization.arap

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

trait HarmonizationEdwColumns {

  val selMaterialColumns: Seq[Column] = Seq(
    col("material").as("material_edw"),
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
    col("zrptmb_description").as("brand_partner"),
    col("bottle_per_case").cast("float")
  )

  val selCustomerColumns: Seq[Column] = Seq(
    col("cust_sales"),
    col("cust_sales_description"),
    col("salesorg"),
    col("distr_chan"),
    col("division"),
    col("sales_dist"),
    col("sales_dist_description"),
    col("country"),
    col("country_description"),
    col("zzcusth2_description"),
    col("zzcusth3_description"),
    col("zzcusth5"),
    col("zzcusth6"),
    col("zzcusth5_description"),
    col("zzcusth6_description"),
    col("zzcusth7"),
    col("zzcusth7_description"),
    col("salesorg_description")
  )

}
