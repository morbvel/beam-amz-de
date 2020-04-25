package com.beamsuntory.amz.de.harmonization.arap

import com.beamsuntory.amz.de.commons.UtilsAmzDE
import com.beamsuntory.bgc.commons.DataFrameUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._


class HarmonizationArap(val utils: UtilsAmzDE) extends HarmonizationArapColumns with HarmonizationArapSeqs {


  def mainExecution(): Unit = {
    harmonizationCleanAlternatePurchase()
    harmonizationCleanAmzTerms()
    harmonizationCleanCustomerReviews()
    harmonizationCleanForecastInventoryOrdered()
    harmonizationCleanForecastInventoryShipped()
    harmonizationCleanGeographicSales()
    harmonizationCleanInventoryHealth()
    harmonizationCleanItemComparison()
    harmonizationCleanSalesOrdered()
    harmonizationCleanSalesShipped()
    harmonizationCleanSalesShippedSourcing()
    harmonizationCleanMarketBastketAnalysis()
    harmonizationSalesDiagnosticUnion()
  }

  def harmonizationCleanSalesOrdered(): Unit = {

    harmonizationCleaning("sales_diagnostic_ordered", salesDiagnosticClean, salesDiagnosticNumeric)
      .select(selSalesOrderedColumns: _*)
      .withColumn("ordered_revenue_last_year", when(col("month") > 6 or (col("month") === 6 and col("day") >28), lit(0.0))
        .otherwise(abs(col("ordered_revenue")) * 100 / (col("ordered_revenue_pct_last_year") + 100)))
      .withColumn("ordered_units_last_year", when(col("month") > 6 or (col("month") === 6 and col("day") >28), lit(0.0))
        .otherwise(abs(col("ordered_units") * 100 / round(col("ordered_units_pct_last_year") + 100)).cast("int")))
      .withColumn("ordered_revenue_prior_period", abs(col("ordered_revenue")) * 100 / (col("ordered_revenue_pct_prior_period") + 100))
      .withColumn("ordered_units_prior_period", abs(col("ordered_units")) * 100 / round(col("ordered_units_pct_prior_period") + 100))
      .withColumn("ordered_units_inc_last_year", col("ordered_units") - col("ordered_units_last_year"))
      .withColumn("ordered_revenue_inc_last_year", col("ordered_revenue") - col("ordered_revenue_last_year"))
      .withColumn("ordered_units_inc_prior_period", col("ordered_units") - col("ordered_units_prior_period"))
      .withColumn("ordered_revenue_inc_prior_period", col("ordered_revenue") - col("ordered_revenue_prior_period"))
      .persist()
      .writeHiveTable(amzDEhar + "sales_diagnostic_ordered_stg")

  }

  def harmonizationCleanSalesShippedSourcing(): Unit = {
    val sales_diagnostic_shipped_sourcing =
      harmonizationCleaning("sales_diagnostic_shipped_sourcing", salesDiagnosticSourcingClean, salesDiagnosticSourcingNumeric)
      .select(selSalesSourcingColumns: _*)
      .withColumn("shipped_revenue_sourcing_last_year",
        round(col("shipped_revenue_sourcing") * 100 / (col("shipped_revenue_pct_last_year_sourcing") + 100)))
      .withColumn("shipped_units_sourcing_last_year",
        round(col("shipped_units_sourcing") * 100 / round(col("shipped_units_pct_last_year_sourcing") + 100)))
      .withColumn("shipped_revenue_sourcing_prior_period",
        round(col("shipped_revenue_sourcing") * 100 / (col("shipped_revenue_pct_prior_period_sourcing") + 100)))
      .withColumn("shipped_units_sourcing_prior_period",
        round(col("shipped_units_sourcing") * 100 / round(col("shipped_units_pct_prior_period_sourcing") + 100)))
      .withColumn("shipped_units_sourcing_inc_last_year", col("shipped_units_sourcing") - col("shipped_units_sourcing_last_year"))
      .withColumn("shipped_revenue_sourcing_inc_last_year", col("shipped_revenue_sourcing") - col("shipped_revenue_sourcing_last_year"))
      .withColumn("shipped_units_sourcing_inc_prior_period", col("shipped_units_sourcing") - col("shipped_units_sourcing_prior_period"))
      .withColumn("shipped_revenue_sourcing_inc_prior_period", col("shipped_revenue_sourcing") - col("shipped_revenue_sourcing_prior_period"))
      .withColumn("distributor_sourcing", lit(1))
      .persist()

    val sales_diagnostic_start_date:DataFrame = sales_diagnostic_shipped_sourcing.join(harmonizationSalesSourcingFlag(sales_diagnostic_shipped_sourcing)
        .persist()
      , Seq("asin")
      , "left_outer")
      .distinct()
      .withColumn("shipped_revenue_sourcing_last_year", when(col("week")<col("start_week"), lit(0.0))
        .otherwise(col("shipped_revenue_sourcing_last_year")))
      .withColumn("shipped_units_sourcing_last_year", when(col("week")<col("start_week"), lit(0))
        .otherwise(col("shipped_units_sourcing_last_year")))
      .drop("start_week")

    harmonizationSalesSourcingLastYear(sales_diagnostic_start_date)
      .writeHiveTable(amzDEhar + "sales_diagnostic_shipped_sourcing" + "_stg")
  }

  def harmonizationCleanSalesShipped(): Unit = {

    val product_catalog_raw: DataFrame = utils.spark.read.table(amzDEraw + "product_catalog")

    val product_catalog = utils.applyUdfRecursiverly(product_catalog_raw, utils.deleteNonUsedChars, productCatalogClean: _*)
        .select(col("asin"),col("ean").cast("bigint"), col("upc"), col("parent_asin"), col("product_title"))
        .persist()

    val sales_diagnostic_shipped = harmonizationCleaning("sales_diagnostic_shipped", salesDiagnosticShippedClean, salesDiagnosticShippedNumeric)
      .select(selSalesShippedColumns: _*)
      .groupBy("asin","start_date", "week", "week_amz", "day", "month", "quarter", "year")
      .agg(
        sum("shipped_revenue").as("shipped_revenue"),
        sum("shipped_revenue_pct_total").as("shipped_revenue_pct_total"),
        sum("shipped_revenue_pct_prior_period").as("shipped_revenue_pct_prior_period"),
        sum("shipped_revenue_pct_last_year").as("shipped_revenue_pct_last_year"),
        sum("shipped_units").as("shipped_units"),
        sum("shipped_units_pct_total").as("shipped_units_pct_total"),
        sum("shipped_units_pct_prior_period").as("shipped_units_pct_prior_period"),
        sum("shipped_units_pct_last_year").as("shipped_units_pct_last_year"),
        sum("customer_returns").as("customer_returns"),
        sum("free_replacements").as("free_replacements"),
        sum("subcategory_sales_rank_shipped").as("subcategory_sales_rank_shipped"),
        sum("subcategory_better_worse_shipped").as("subcategory_better_worse_shipped"),
        sum("avg_sales_price_shipped").as("avg_sales_price_shipped"),
        sum("avg_sales_price_pct_prior_period_shipped").as("avg_sales_price_pct_prior_period_shipped"),
        sum("change_gv_pct_prior_period").as("change_gv_pct_prior_period"),
        sum("change_gv_pct_last_year").as("change_gv_pct_last_year"),
        sum("rep_oss_pct").as("rep_oss_pct"),
        sum("rep_oss_pct_total").as("rep_oss_pct_total"),
        sum("rep_oss_pct_prior_period").as("rep_oss_pct_prior_period"),
        sum("lbb_pct").as("lbb_pct")
      )
      .withColumn("shipped_revenue_last_year", when(col("month") > 6 or (col("month") === 6 and col("day") >28), lit(0.0))
        .otherwise(round(col("shipped_revenue") * 100 / (col("shipped_revenue_pct_last_year") + 100))))
      .withColumn("shipped_units_last_year", when(col("month") > 6 or (col("month") === 6 and col("day") >28), lit(0.0))
        .otherwise(round(col("shipped_units") * 100 / round(col("shipped_units_pct_last_year") + 100))))
      .withColumn("shipped_revenue_prior_period", round(col("shipped_revenue") * 100 / (col("shipped_revenue_pct_prior_period") + 100)))
      .withColumn("shipped_units_prior_period", round(col("shipped_units") * 100 / round(col("shipped_units_pct_prior_period") + 100)))
      .withColumn("shipped_units_inc_last_year", col("shipped_units") - col("shipped_units_last_year"))
      .withColumn("shipped_revenue_inc_last_year", col("shipped_revenue") - col("shipped_revenue_last_year"))
      .withColumn("shipped_units_inc_prior_period", col("shipped_units") - col("shipped_units_prior_period"))
      .withColumn("shipped_revenue_inc_prior_period", col("shipped_revenue") - col("shipped_revenue_prior_period"))
      .persist()

    sales_diagnostic_shipped
      .join(product_catalog, Seq("asin"), "left_outer")
      .writeHiveTable(amzDEhar + "sales_diagnostic_shipped_stg")

  }

  def harmonizationCleanAlternatePurchase(): Unit = {
    val alternatePurchase = harmonizationCleaning("alternate_purchase", alternatePurchaseClean, alternatePurchaseNumeric)
    harmonizationTranspose(alternatePurchase, 4,alternatePurchaseTransposeTransBy,
      alternatePurchaseTransposeTransOf)
      .select(selAlternatePurchaseColumns: _*)
      .writeHiveTable(amzDEhar + "alternate_purchase" + "_stg")

  }

  def harmonizationCleanAmzTerms(): Unit = {
    val amzTerms = harmonizationCleaning("amz_terms", amzTermsClean, amzTermsNumeric)
    harmonizationTranspose(amzTerms, 2, amzTermsTransposeTransBy,
      amzTermsTransposeTransOf)
      .select(selAmzTermsColumns: _*)
      .writeHiveTable(amzDEhar + "amz_terms" + "_stg")

  }

  def harmonizationCleanCustomerReviews(): Unit = {
    harmonizationCleaning("customer_reviews", customerReviewsClean, customerReviewsNumeric)
      .select(selCustomerReviewsColumns: _*)
      .writeHiveTable(amzDEhar + "customer_reviews" + "_stg")

  }

  def harmonizationCleanForecastInventoryShipped(): Unit = {
    val forecastInventoryShipped = harmonizationCleaning("forecast_inventory_shipped", forecastShippedInventoryClean, forecastShippedInventoryNumeric)
    utils.transposeUDF(forecastInventoryShipped, forecastShippedInventoryTranspose)
      .withColumn("model", splitWeekModel(col("week_model")))
      .withColumn("week_model", splitWeek(col("week_model")))
      .select(selForecastShippedColumns: _*)
      .writeHiveTable(amzDEhar + "forecast_inventory_shipped" + "_stg")

  }

  def harmonizationCleanForecastInventoryOrdered(): Unit = {
    val forecastInventoryOrdered =  harmonizationCleaning("forecast_inventory_ordered", forecastInventoryClean, forecastInventoryNumeric)

    val forecastInventoryOrdered_stg = utils.transposeUDF(forecastInventoryOrdered, forecastOrderedInventoryTranspose)
      .withColumn("model", splitWeekModel(col("week_model")))
      .withColumn("week_model", splitWeek(col("week_model")))
      .select(selForecastOrderedColumns: _*)
      .persist()

    val lastdayInventory =  forecastInventoryOrdered_stg.select("day", "month","year")
      .groupBy("month", "year")
      .agg(max("day").as("day"))
      .withColumn("last_day_inventory", lit(1) )
        .persist()

    forecastInventoryOrdered_stg.join(lastdayInventory, Seq("year", "month", "day") , "left_outer")
      .writeHiveTable(amzDEhar + "forecast_inventory_ordered" + "_stg")
  }

  def harmonizationCleanGeographicSales(): Unit = {

    val geographic_sales_insights = harmonizationCleaning("geographic_sales_insights", geographicSalesClean, geographicSalesNumeric)
      .select(selGeographicSales: _*)
    val germany_cities: DataFrame = utils.spark.read.table(amzDEraw + "germany_cities")

    val geographic_sales_insights_stg: DataFrame = geographic_sales_insights.join(germany_cities.select(col("latitude"),
      col("longitude"),col("postal_codes").alias("plz")),Seq("plz"), "left_outer")

    geographic_sales_insights_stg
      .drop("ean")
      .drop("product_title")
      .join(harmonizationAsinEanTable(geographic_sales_insights_stg), Seq("asin"), "left_outer")
      .writeHiveTable(amzDEhar + "geographic_sales_insights" + "_stg")

  }

  def harmonizationCleanInventoryHealth(): Unit = {
    harmonizationCleaning("inventory_health", inventoryHealthClean, inventoryHealthNumeric)
      .select(selInventoryHealthColumns: _*)
      .writeHiveTable(amzDEhar + "inventory_health" + "_stg")

  }

  def harmonizationCleanItemComparison(): Unit = {
    val itemComparison = harmonizationCleaning("item_comparison", itemComparisonClean, itemComparisonNumeric)
    harmonizationTranspose(itemComparison, 4, itemComparisonTransBy,
      itemComparisonTransOf)
      .select(selItemComparisonColumns: _*)
      .writeHiveTable(amzDEhar + "item_comparison" + "_stg")

  }

  def harmonizationCleanMarketBastketAnalysis(): Unit = {
    val marketBastketAnalysis = harmonizationCleaning("market_basket_analysis", marketBasketAnalysisClean, marketBasketAnalysisNumeric)
    harmonizationTranspose(marketBastketAnalysis, 2, marketBasketAnalysisTransBy,
      marketBasketAnalysisTransOf)
      .select(selMarketBasketAnalysisColumns: _*)
      .writeHiveTable(amzDEhar + "market_basket_analysis" + "_stg")

  }

  def harmonizationSalesDiagnosticUnion(): Unit = {
    val sales_diagnostic_ordered = utils.spark.read.table(amzDEhar + "sales_diagnostic_ordered_stg")
    val sales_diagnostic_shipped = utils.spark.read.table(amzDEhar + "sales_diagnostic_shipped_stg")
    val sales_diagnostic_shipped_sourcing = utils.spark.read.table(amzDEhar + "sales_diagnostic_shipped_sourcing_stg")

    // Flag for weeks with no sales
    val sales_diagnostic: DataFrame = sales_diagnostic_ordered.alias("ord").join(
      sales_diagnostic_shipped.alias("shi"), Seq("asin", "start_date", "week", "week_amz", "month", "day", "year", "quarter"), "full_outer")
      .select(selSalesDiagnosticUnionColumns: _*)

    val sales_diagnostic_start_date:DataFrame = sales_diagnostic.join(harmonizationSalesDiagnosticFlag(sales_diagnostic)
        , Seq("asin")
        , "left_outer")
      .distinct()
      .withColumn("shipped_revenue_last_year", when(col("week")<col("start_week"), lit(0.0))
        .otherwise(col("shipped_revenue_last_year")))
      .withColumn("shipped_units_last_year", when(col("week")<col("start_week"), lit(0))
        .otherwise(col("shipped_units_last_year")))
      .withColumn("ordered_revenue_last_year", when(col("week")<col("start_week"), lit(0.0))
        .otherwise(col("ordered_revenue_last_year")))
      .withColumn("ordered_units_last_year", when(col("week")<col("start_week"), lit(0))
        .otherwise(col("ordered_units_last_year")))
      .drop("start_week")

    val sales_diagnostic_shipped_ordered = harmonizationSalesLastYear(sales_diagnostic_start_date
      .drop("ean")
      .drop("product_title")
      .join(harmonizationAsinEanTable(sales_diagnostic_start_date), Seq("asin"), "left_outer"))

    val sales_diagnostic_full: DataFrame = sales_diagnostic_shipped_ordered
      .alias("ord")
      .join(sales_diagnostic_shipped_sourcing.alias("shi"), Seq("asin", "start_date", "week", "week_amz", "month", "day", "year", "quarter"), "full_outer")
      .select(selSalesDiagnosticUnionSourcingColumns: _*)
      .withColumn("distributor_sourcing", when(col("distributor_sourcing").isNull, lit(0))
      .otherwise(col("distributor_sourcing")))
      .withColumn("distributor_non_sourcing", lit(1))

    sales_diagnostic_full
      .drop("ean")
      .drop("product_title")
      .join(harmonizationAsinEanTable(sales_diagnostic_full), Seq("asin"), "left_outer")
      .distinct()
      .writeHiveTable(amzDEhar + "sales_diagnostic")
  }

  def harmonizationSalesSourcingLastYear(sales_diagnostic_start_date: DataFrame): DataFrame = {

    val sales_diagnostic_shipped_sourcing_ly = sales_diagnostic_start_date
      .select(selSalesDiagnosticSourcingLy: _*)
      .filter(col("year") === 2017)
      .filter(col("month") > 8 or (col("month") === 8 and col("day") >16))
      .withColumn("year", col("year") + 1)


    val sales_diagnostic_start_date_full = sales_diagnostic_start_date
      .join(sales_diagnostic_shipped_sourcing_ly, Seq("asin","month", "quarter", "day", "year"), "left_outer")
      .withColumn("shipped_revenue_sourcing_last_year",
        when(col("shipped_revenue_sourcing_last_year_aux").isNotNull, col("shipped_revenue_sourcing_last_year_aux"))
          .otherwise(col("shipped_revenue_sourcing_last_year")))
      .withColumn("shipped_units_sourcing_last_year",
        when(col("shipped_units_sourcing_last_year_aux").isNotNull, col("shipped_units_sourcing_last_year_aux"))
          .otherwise(col("shipped_units_sourcing_last_year")))
      .drop("shipped_units_sourcing_last_year_aux")
      .drop("shipped_revenue_sourcing_last_year_aux")

    val sales_diagnostic_shipped_sourcing_ly_calculated: DataFrame = sales_diagnostic_start_date
      .select(selSalesDiagnosticSourcingLyCalculated: _*)
      .filter(col("year") === 2018)
      .filter(col("month") < 8 or (col("month") === 8 and col("day") <17))
      .withColumn("year", col("year") - 1)
      .withColumn("start_date", concat(col("year"), lit("-"), col("month"), lit("-"), col("day")).cast("date"))

    sales_diagnostic_start_date_full
      .fullUnion(sales_diagnostic_shipped_sourcing_ly_calculated)
      .distinct()
  }

  def harmonizationSalesLastYear(sales_diagnostic_start_date: DataFrame): DataFrame = {

    val sales_diagnostic_ly = sales_diagnostic_start_date
      .filter(col("year") === 2017)
      .select(selSalesDiagnosticLy: _*)
      .filter(col("month") > 6 or (col("month") === 6 and col("day") >28))
      .withColumn("year", col("year") + 1)
      .withColumn("ly", lit(1))

    val sales_diagnostic_start_date_full = sales_diagnostic_start_date
      .join(sales_diagnostic_ly, Seq("asin","ean","month", "quarter", "day", "year"), "left_outer")
      .withColumn("shipped_revenue_last_year",
        when(col("shipped_revenue_last_year_aux").isNotNull, col("shipped_revenue_last_year_aux"))
          .otherwise(col("shipped_revenue_last_year")))
      .withColumn("shipped_units_last_year",
        when(col("shipped_units_last_year_aux").isNotNull, col("shipped_units_last_year_aux"))
          .otherwise(col("shipped_units_last_year")))
      .withColumn("ordered_revenue_last_year",
        when(col("ordered_revenue_last_year_aux").isNotNull, col("ordered_revenue_last_year_aux"))
          .otherwise(col("ordered_revenue_last_year")))
      .withColumn("ordered_units_last_year",
        when(col("ordered_units_last_year_aux").isNotNull, col("ordered_units_last_year_aux"))
          .otherwise(col("ordered_units_last_year")))

    val sales_diagnostic_ly_calculated : DataFrame = sales_diagnostic_start_date.select(selSalesDiagnosticLyCalculated: _*)
      .filter(col("year") === 2018)
      .filter(col("month") < 6 or (col("month") === 6 and col("day") < 29))
      .withColumn("year", col("year") - 1)
      .withColumn("start_date", concat(col("year"), lit("-"), col("month"), lit("-"), col("day")).cast("date"))

    sales_diagnostic_start_date_full.fullUnion(sales_diagnostic_ly_calculated)
      .distinct()
  }

  def harmonizationSalesSourcingFlag(sales_diagnostic_sourcing: DataFrame): DataFrame = {

    val sales_diagnostic_sourcing_flag_tmp: DataFrame = sales_diagnostic_sourcing
      .select(col("asin"), col("week"),col("shipped_revenue_pct_last_year_sourcing"))
      .filter(col("year") === lit(2018))
      .groupBy(col("asin"), col("week"))
      .agg(sum(col("shipped_revenue_pct_last_year_sourcing")).as("sum_results"))
      .withColumn("sum_results",
        when(col("sum_results").isNull, lit(0.0))
          .otherwise(col("sum_results")))
      .withColumn("flag", when(col("sum_results") === lit(0.0), lit(0)).otherwise(lit(1)))
      .drop("sum(shipped_revenue_pct_last_year)")
      .drop("sum(ordered_revenue_pct_last_year)")

    val sales_diagnostic_sourcing_flag_week = sales_diagnostic_sourcing_flag_tmp
      .filter(col("flag").equalTo(1))
      .groupBy(col("asin"))
      .agg(min("week").as("start_week"))
      .drop("week")

    sales_diagnostic_sourcing_flag_tmp.select("asin").distinct()
      .join(sales_diagnostic_sourcing_flag_week, Seq("asin"), "left_outer")
      .withColumn("start_week", when(col("start_week").isNull, 55)
        .otherwise(col("start_week")))
  }

  def harmonizationSalesDiagnosticFlag(sales_diagnostic: DataFrame): DataFrame = {

    val sales_diagnostic_flag_tmp: DataFrame = sales_diagnostic
      .select(col("asin"), col("week"),col("shipped_revenue_pct_last_year"), col("ordered_revenue_pct_last_year"))
      .filter(col("year") === lit(2018))
      .groupBy(col("asin"), col("week"))
      .agg(sum(col("shipped_revenue_pct_last_year")),
        sum(col("ordered_revenue_pct_last_year")))
      .withColumn("sum_results", col("sum(shipped_revenue_pct_last_year)") + col("sum(ordered_revenue_pct_last_year)") )
      .withColumn("sum_results",
        when(col("sum_results").isNull, lit(0.0))
          .otherwise(col("sum_results")))
      .withColumn("flag", when(col("sum_results") === lit(0.0), lit(0)).otherwise(lit(1)))
      .drop("sum(shipped_revenue_pct_last_year)")
      .drop("sum(ordered_revenue_pct_last_year)")

    val sales_diagnostic_flag_week = sales_diagnostic_flag_tmp
      .filter(col("flag").equalTo(1))
      .groupBy(col("asin"))
      .agg(min("week").as("start_week"))
      .drop("week")

    sales_diagnostic_flag_tmp.select("asin").distinct()
      .join(sales_diagnostic_flag_week, Seq("asin"), "left_outer")
      .withColumn("start_week", when(col("start_week").isNull, 55)
        .otherwise(col("start_week")))
  }

  def harmonizationAsinEanTable(dataframe_arap_reduce: DataFrame): DataFrame = {
    val dataframe_arap_reduce_ean: DataFrame = dataframe_arap_reduce
      .select("asin", "start_date", "ean", "product_title")
      .distinct()
    val dataframe_arap_max_asin: DataFrame = dataframe_arap_reduce_ean.select(
      col("asin").as("asin"), col("start_date"))
      .groupBy("asin")
      .agg(max("start_date").as("start_date"))

    dataframe_arap_max_asin
      .join(dataframe_arap_reduce_ean, Seq("asin", "start_date"), "left_outer")
      .drop("start_date")
  }

  def harmonizationCleaning(rawTable: String, columnsClean: Seq[String], columnsNumeric: Seq[String]): DataFrame = {

    val raw_file: DataFrame = utils.spark.read.table(amzDEraw + rawTable)
    utils.applyUdfRecursiverly(
      utils.createWeekOfYear(
        utils.applyUdfRecursiverly(raw_file, utils.deleteNonUsedChars, columnsClean: _*))
      , utils.castToNumeric, columnsNumeric: _*)
      .filter(col("start_date").isNotNull)
      .withColumn("month", substring(col("start_date").cast("string"),6 , 2).cast("int"))
      .withColumn("year", substring(col("start_date").cast("string"),1 , 4).cast("int"))
      .withColumn("day", substring(col("start_date").cast("string"),9 , 2).cast("int"))
      .withColumn("quarter", when(col("month") < 4 , 1)
        .when(col("month") > 3 and col("month")< 7, 2)
        .when(col("month") > 6 and col("month")< 10, 3)
        .otherwise(4)
      )
  }

  def harmonizationTranspose(df: DataFrame, limitNum: Int, transBy: Seq[String], transOf: Seq[String]): DataFrame = {
    val listNum = List("first", "second", "third", "fourth", "fifth")

    val colTransBy = transBy.map(col(_))
    val colTransOfFirst = transOf.map(x => col(listNum(0) + "_" + x).alias(x))
    val unionColTransFirst = colTransBy ++ colTransOfFirst
    var dfFirst = df.select(unionColTransFirst: _*)
      .withColumn("type", lit(listNum(0)))

    for (a <- 1 to limitNum) {
      val colTransOfNext = transOf.map(x => col(listNum(a) + "_" + x).alias(x))
      val unionColTransNext = colTransBy ++ colTransOfNext

      val dfNext = df.select(unionColTransNext: _*)
        .withColumn("type", lit(listNum(a)))
      dfFirst = dfFirst.fullUnion(dfNext)
    }
    dfFirst
  }

  val splitWeekModel: UserDefinedFunction = udf { str: String =>
    val strSplit = str.split("_")
    strSplit(2) + " " + strSplit(3)
  }

  val splitWeek: UserDefinedFunction = udf { str: String =>
    val strSplit = str.split("_")
    strSplit(1)
  }
}