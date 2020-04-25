package com.beamsuntory.amz.de.functional.arap

import com.beamsuntory.amz.de.commons.UtilsAmzDE
import com.beamsuntory.bgc.commons.DataFrameUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._


class FunctionalArapEdw(val utils: UtilsAmzDE) extends FunctionalArapEdwColumns {

  val important_cities: List[String] = utils.config.getStringList("germany_cities").toList
  import utils.spark.implicits._

  def mainExecution(): Unit = {
    createMaterialTableWithHierarchy
    functionalEdw
  }

  def createMaterialTableWithHierarchy(): Unit = {

    val edw_material: DataFrame =  utils.spark.read.table(amzDEraw + "edw_material").select(selMaterialColumns: _*)
    val material_amazon: DataFrame = utils.spark.read.table(amzDEraw + "material_amazon")
      .filter(col("eanupc").isNotNull)
      //.filter(col("alternative_unit").equalTo("BOT") || col("alternative_unit").equalTo("CS") )
      .select( col("eanupc"), col("material"))
      .withColumn("eanupc", col("eanupc").cast("String"))
      .withColumnRenamed("eanupc", "ean")

    val join_materials: DataFrame = material_amazon.join(edw_material
      , trim(material_amazon.col("material"))===trim(edw_material.col("material"))
      , "left_outer")
      .filter(col("prodh3_description").isNotNull)
      .filter(col("brand_partner") =!= "")
      .drop(edw_material("material"))

    val join_material_min: DataFrame = join_materials
        .select("ean", "material")
      .groupBy("ean")
      .agg(max("material").as("material"))

    // Join material with hierarchy
    val asin_product_hierarchy: DataFrame = utils.spark.read.table(amzDEraw + "asin_product_hierarchy_old").drop(col("asin"))
        .filter(col("eanupc").isNotNull)
        .withColumnRenamed("eanupc", "ean")

    join_materials.join(join_material_min, Seq("material", "ean"), "inner")
      .drop("min_material")
      .fullUnion(asin_product_hierarchy)
      .withColumn("brand_partner", when(col("brand_partner") === lit("not applicable"), "UNASSIGNED")
          .when(col("brand_partner") === lit(""), "UNASSIGNED")
        .otherwise(upper(col("brand_partner"))))
      .distinct()
      .writeHiveTable(amzDEfun + "edw_material_hierarchy_amazon")
  }

  def functionalEdw(): Unit = {

    val sales_diagnostic: DataFrame = functionalSalesDiagnostic()

    createTwelveMonthColumn(sales_diagnostic).writeHiveTable(amzDEfun + "sales_diagnostic")

    createTwelveMonthColumn(geographicSalesInsights(sales_diagnostic))
      .writeHiveTable(amzDEfun + "geographic_sales_insights")

  }

  def functionalSalesDiagnostic(): DataFrame = {
    joinMaterialSales(utils.spark.read.table(amzDEhar + "sales_diagnostic"))
      .withColumn("product_title", utils.singleSpace(col("product_title")))
      .withColumn("asin", trim(col("asin")))
      .withColumn("prodh1_description",when( col("prodh1_description").isNull, lit("UNASSIGNED") )
        .otherwise(col("prodh1_description")))
      .withColumn("prodh2_description",when( col("prodh2_description").isNull, lit("UNASSIGNED") )
        .otherwise(col("prodh2_description")))
      .withColumn("prodh3_description",when( col("prodh3_description").isNull, lit("UNASSIGNED FAMILY") )
        .otherwise(col("prodh3_description")))
      .withColumn("prodh4_description",when( col("prodh4_description").isNull, lit("UNASSIGNED BRAND") )
        .otherwise(col("prodh4_description")))
      .withColumn("prodh5_description",when( col("prodh5_description").isNull, lit("UNNASSIGNED") )
        .otherwise(col("prodh5_description")))
      .withColumn("prodh6_description",when( col("prodh6_description").isNull, lit("UNNASSIGNED") )
        .otherwise(col("prodh6_description")))
      .withColumn("brand_partner", when(col("brand_partner").isNull, "UNASSIGNED")
        .otherwise(upper(col("brand_partner"))))
      .withColumn("month_str",utils.mappingMonthNameUDF(col("month")))
      .withColumn("volume_9L_shipped", col("num_bottles")*col("size")*col("shipped_units")/9000)
      .withColumn("volume_9L_ordered", col("num_bottles")*col("size")*col("ordered_units")/9000)
      .withColumn("volume_9L_shipped_sourcing", col("num_bottles")*col("size")*col("shipped_units_sourcing")/9000)
      .withColumn("volume_07L_shipped", col("num_bottles")*col("size")*col("shipped_units")/700)
      .withColumn("volume_07L_ordered", col("num_bottles")*col("size")*col("ordered_units")/700)
      .withColumn("volume_07L_shipped_sourcing", col("num_bottles")*col("size")*col("shipped_units_sourcing")/700)
      .withColumn("volume_9L_shipped_last_year", col("num_bottles")*col("size")*col("shipped_units_last_year")/9000)
      .withColumn("volume_9L_ordered_last_year", col("num_bottles")*col("size")*col("ordered_units_last_year")/9000)
      .withColumn("volume_9L_shipped_sourcing_last_year", col("num_bottles")*col("size")*col("shipped_units_sourcing_last_year")/9000)
      .withColumn("volume_07L_shipped_last_year", col("num_bottles")*col("size")*col("shipped_units_last_year")/700)
      .withColumn("volume_07L_ordered_last_year", col("num_bottles")*col("size")*col("ordered_units_last_year")/700)
      .withColumn("volume_07L_shipped_sourcing_last_year", col("num_bottles")*col("size")*col("shipped_units_sourcing_last_year")/700)
      .withColumn("volume_9L_customer_returns", col("num_bottles")*col("size")*col("customer_returns")/9000)
      .withColumn("volume_07L_customer_returns", col("num_bottles")*col("size")*col("customer_returns")/700)
      .withColumn("volume_9L_customer_returns_sourcing", col("num_bottles")*col("size")*col("customer_returns_sourcing")/9000)
      .withColumn("volume_07L_customer_returns_sourcing", col("num_bottles")*col("size")*col("customer_returns_sourcing")/700)
      .withColumn("volumen_9L_free_replacements", col("num_bottles")*col("size")*col("free_replacements")/9000)
      .withColumn("volume_07L_free_replacements", col("num_bottles")*col("size")*col("free_replacements")/700)
      .withColumn("volumen_9L_free_replacements_sourcing", col("num_bottles")*col("size")*col("free_replacements_sourcing")/9000)
      .withColumn("volume_07L_free_replacements_sourcing", col("num_bottles")*col("size")*col("free_replacements_sourcing")/700)
      .distinct()
  }

  def geographicSalesInsights(sales_diagnostic: DataFrame): DataFrame = {
    val geographic_sales: DataFrame = joinMaterialSales(utils.spark.read.table(amzDEhar + "geographic_sales_insights_stg"))
      .withColumn("product_title", utils.singleSpace(col("product_title")))

    val germany_cities_df: DataFrame = cityLatitudeLongitude(geographic_sales)

    geographic_sales
      .withColumn("city", when(col("city").isNull, lit("OTHERS"))
        .otherwise(upper(col("city"))))
      .withColumn("city", utils.mapping_cities_geographic(col("city")))
        .join(germany_cities_df, Seq("city"), "left_outer")
      .join(sales_diagnostic
      .withColumn("ordered_shipped_units_conversion_rate", col("ordered_units") / col("shipped_units"))
      .withColumn("ordered_shipped_revenue_conversion_rate", col("ordered_revenue") / col("shipped_revenue"))
      .select(selSalesColumns: _*), Seq("asin", "start_date"), "left_outer")
      .drop(sales_diagnostic("asin"))
      .drop(sales_diagnostic("start_date"))
      .withColumn("state", when(col("state").isNull, lit("OTHERS"))
        .otherwise(upper(col("state"))))
      .withColumn("plz", when(col("plz").isNull, lit("OTHERS"))
        .otherwise(upper(col("plz"))))
      .withColumn("ordered_units", round(col("shipped_units") * col("ordered_shipped_units_conversion_rate")))
      .withColumn("ordered_revenue", col("shipped_revenue") * col("ordered_shipped_revenue_conversion_rate"))
      .withColumn("month_str",utils.mappingMonthNameUDF(col("month")))
      .withColumn("volume_9L_shipped", col("num_bottles")*col("size")*col("shipped_units")/9000)
      .withColumn("volume_9L_ordered", col("num_bottles")*col("size")*col("ordered_units")/9000)
      .withColumn("volume_07L_shipped", col("num_bottles")*col("size")*col("shipped_units")/700)
      .withColumn("volume_07L_ordered", col("num_bottles")*col("size")*col("ordered_units")/700)
      .distinct()
  }

  def cityLatitudeLongitude(geographic: DataFrame): DataFrame = {
    geographic
        .filter(col("city")isin(important_cities: _*))
      .select("city", "latitude", "longitude")
      .withColumnRenamed("latitude", "latitude_city")
      .withColumnRenamed("longitude", "longitude_city")
  }

  def joinMaterialSales(dataframe_arap: DataFrame): DataFrame = {

    val join_material_hierarchy: DataFrame = utils.spark.read.table(amzDEfun + "edw_material_hierarchy_amazon")

    // Join material_hierarchy with sales
    val dataframe_arap_clean = dataframe_arap
      .withColumn("ean", trim(col("ean").cast("String")))
      .withColumn("ean", when(col("ean").isNull, lit("0"))
        .otherwise(col("ean")))

    dataframe_arap_clean.join(join_material_hierarchy
      , Seq("ean")
      , "left_outer")
      .withColumn("size", when((col("size").isNull || trim(col("size")).equalTo("0"))
        && col("product_title").contains(regexp_extract(col("product_title"), "\\(([^)]+)\\)", 1)),
        split(regexp_extract(col("product_title"), "\\(([^)]+)\\)", 1), " ")(2)*1000 )
        .otherwise(col("size")))
      .withColumn("size_1", when((col("size").isNull || trim(col("size")).equalTo("0"))
        && col("product_title").contains(regexp_extract(col("product_title"), "\\(([^)]+)\\)", 1)),
        split(regexp_extract(col("product_title"), "\\(([^)]+)\\)", 1), " ")(2)*1000 )
        .otherwise(col("size_1")))
      .withColumn("size", trim(col("size")).cast("integer"))
      .withColumn("size_1", trim(col("size_1")).cast("integer"))
      .withColumn("num_bottles_regex", split(trim(split(regexp_extract(col("product_title"), "\\(([^)]+)\\)", 1), "x")(0)), " ")(0))
      .withColumn("num_bottles", when(col("num_bottles_regex").isNull || col("num_bottles_regex").cast("int") > 1000, 1)
        .otherwise(col("num_bottles_regex").cast("int")))
      .withColumn("size", when( col("size").isNull || trim(col("size")).equalTo("0"), lit("UNASSIGNED") ).otherwise(col("size")) )
      .withColumn("size_1", when( col("size_1").isNull || trim(col("size_1")).equalTo("0"), lit("UNASSIGNED") ).otherwise(col("size_1")) )
      .withColumn("ean", when( trim(col("ean")).equalTo("0"), lit("UNASSIGNED") ).otherwise(col("ean")) )
  }

  def createTwelveMonthColumn(df: DataFrame) : DataFrame = {

    val row_twelve_month = df.select("year", "month")
      .distinct()
      .orderBy(desc("year"), desc("month"))
      .take(12)

    val df_twelve_month = utils.spark.sparkContext.parallelize(row_twelve_month).map(
      row => (row.getInt(0), row.getInt(1)))
      .toDF("year", "month")
        .withColumn("last_twelve_month", lit(1))

    df.join(df_twelve_month, Seq("year", "month"), "left_outer")

  }
}