package com.beamsuntory.amz.de.functional.ams

import com.beamsuntory.amz.de.commons.UtilsAmzDE
import com.beamsuntory.amz.de.functional.arap.FunctionalAmsEdwColumns
import com.beamsuntory.bgc.commons.DataFrameUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


class FunctionalRadarAms(val utils: UtilsAmzDE) extends FunctionalRadarAmsColumns {


  def mainExecution(): Unit = {
    functionalRadar
  }

  def functionalRadar(): Unit = {
    val campaign_table = utils.spark.read.table(amzDEhar + "campaigns_summary")
      .select(selProdcutKeywordColumns: _*)

    val hierarchy_campaigns: DataFrame = utils.spark.read.table(amzDEfun + "hierarchy_campaigns")

    val campaign_table_hierarchy = campaign_table.join(hierarchy_campaigns ,Seq("campaign_name") ,"left_outer")
      .withColumn("item_name", trim(split(col("campaign_name"), "-").getItem(1)))
      .withColumn("campaign_hierarchy", utils.mapping_hierarchy_campaigns_hsa(col("item_name")))
      .withColumn("prodh2_description", when(col("prodh2_description").isNotNull, col("prodh2_description"))
        .otherwise(split(col("campaign_hierarchy"), "-").getItem(0)))
      .withColumn("prodh3_description", when(col("prodh3_description").isNotNull, col("prodh3_description"))
        .otherwise(split(col("campaign_hierarchy"), "-").getItem(1)))
      .withColumn("prodh4_description", when(col("prodh4_description").isNotNull, col("prodh4_description"))
        .otherwise(split(col("campaign_hierarchy"), "-").getItem(2)))
      .withColumn("brand_partner", when(col("brand_partner").isNotNull, col("brand_partner"))
        .otherwise(split(col("campaign_hierarchy"), "-").getItem(3)))
      .withColumn("brand_partner", when(col("brand_partner")==="THIRD PARTY", lit("THIRD-PARTY"))
        .otherwise(col("brand_partner")))

    functionalTraspose(campaign_table_hierarchy).writeHiveTable(amzDEfun + "campaigns_summary_radar")

  }

  def functionalTraspose(df: DataFrame): DataFrame = {
    val type_date = List("cost_of_campaign", "marketing_campaign_orders", "impressions", "marketing_campaign_revenue", "clicks", "cost_of_campaign")
    val naming = List("Cost", "Orders", "Impressions", "Revenue", "Clicks", "Cost")

    val x_point = List(-127, 127, 205, 0, -205, -127)
    val y_point = List(-174, -174, 68, 217, 68, -174)

    val BaseColumns = selBaseColumns.map(col(_))
    val columnsToSelFirst = BaseColumns ++ List(type_date(0)).map(col(_))

    var dfFirst = df.select(columnsToSelFirst: _*)
      .withColumnRenamed(type_date(0), "result")
      .withColumn("result_type", lit(naming(0)))
      .withColumn("x_point", lit(x_point(0)))
      .withColumn("y_point", lit(y_point(0)))
      .withColumn("point", lit(1))

    for (a <- 1 to 5) {
      val columnsToSelNext = BaseColumns ++ List(type_date(a)).map(col(_))

      val dfNext = df.select(columnsToSelNext: _*)
        .withColumnRenamed(type_date(a), "result")
        .withColumn("result_type", lit(naming(a)))
        .withColumn("x_point", lit(x_point(a)))
        .withColumn("y_point", lit(y_point(a)))
        .withColumn("point", lit(a + 1))

      dfFirst = dfFirst.fullUnion(dfNext)
    }

    dfFirst
  }

}
