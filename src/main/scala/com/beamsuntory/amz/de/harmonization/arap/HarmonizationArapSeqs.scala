package com.beamsuntory.amz.de.harmonization.arap

trait HarmonizationArapSeqs {


  val salesDiagnosticClean: Seq[String] = Seq("asin", "parent_asin", "product_title",
    "brand", "subcategory", "category", "ordered_revenue", "ordered_revenue_percentage_total",
    "ordered_revenue_prior_period", "ordered_revenue_last_year", "ordered_units_percentage_total",
    "ordered_units_prior_period", "ordered_units_last_year", "subcategory_better_worse", "average_sales_price",
    "average_sales_price_prior_period", "change_in_gv_prior_period", "change_in_gv_last_year", "rep_oos",
    "rep_oos_percentage_total", "rep_oos_prior_period", "lbb")

  val salesDiagnosticNumeric: Seq[String] = Seq("ordered_revenue", "ordered_revenue_percentage_total",
    "ordered_revenue_prior_period", "ordered_revenue_last_year", "ordered_units", "ordered_units_percentage_total",
    "ordered_units_prior_period", "ordered_units_last_year", "subcategory_sales_rank", "subcategory_better_worse",
    "average_sales_price", "average_sales_price_prior_period", "change_in_gv_prior_period", "change_in_gv_last_year",
    "rep_oos", "rep_oos_percentage_total", "rep_oos_prior_period", "lbb")

  val salesDiagnosticShippedClean: Seq[String] = Seq("asin", "parent_asin", "product_title",
    "brand", "subcategory", "category", "shipped_revenue", "shipped_revenue_percentage_total",
    "shipped_revenue_priod_period", "shipped_revenue_last_year", "shipped_units_percentage_total",
    "ordered_units_priod_period", "ordered_units_last_year", "ordered_units_percentage_total",
    "shipped_units_priod_period", "shipped_units_last_year", "subcategory_better_worse", "average_sales_price",
    "average_sales_price_prior_period", "change_in_gv_prior_period", "change_in_gv_last_year", "rep_oos",
    "rep_oos_percentage_total", "rep_oos_prior_period", "lbb")

  val salesDiagnosticShippedNumeric: Seq[String] = Seq("shipped_revenue", "shipped_revenue_percentage_total",
    "shipped_revenue_priod_period", "shipped_revenue_last_year", "shipped_units", "shipped_units_percentage_total",
    "ordered_units", "ordered_units_priod_period", "ordered_units_last_year", "ordered_units_percentage_total",
    "shipped_units_priod_period", "shipped_units_last_year", "customer_returns", "average_sales_price",
    "free_replacements", "subcategory_sales_rank", "subcategory_better_worse",
    "average_sales_price_prior_period", "change_in_gv_prior_period", "change_in_gv_last_year", "rep_oos",
    "rep_oos_percentage_total", "rep_oos_prior_period", "lbb")

  val salesDiagnosticSourcingClean: Seq[String] = Seq("asin", "parent_asin", "product_title",
    "brand", "subcategory", "category", "shipped_revenue_sourcing", "shipped_revenue_percentage_total_sourcing",
    "shipped_revenue_priod_period_sourcing", "shipped_revenue_last_year_sourcing", "shipped_units_percentage_total_sourcing",
    "shipped_units_priod_period_sourcing", "shipped_units_last_year_sourcing", "average_sales_price_sourcing", "average_sales_price_prior_period_sourcing")

  val salesDiagnosticSourcingNumeric: Seq[String] = Seq("shipped_revenue_sourcing", "shipped_revenue_percentage_total_sourcing",
    "shipped_revenue_priod_period_sourcing", "shipped_revenue_last_year_sourcing", "shipped_units_sourcing",
    "shipped_units_percentage_total_sourcing", "shipped_units_priod_period_sourcing", "shipped_units_last_year_sourcing",
    "customer_returns_sourcing", "average_sales_price_sourcing", "free_replacements_sourcing",
    "average_sales_price_prior_period_sourcing")

  val geographicSalesClean: Seq[String] = Seq("asin", "parent_asin", "product_title",
    "brand", "subcategory", "category", "country", "state", "city", "shipped_revenue", "shipped_revenue_percentage_total",
    "shipped_revenue_priod_period", "shipped_revenue_last_year", "shipped_units_percentage_total",
    "shipped_units_priod_period", "shipped_units_last_year", "average_sales_price",
    "average_sales_price_prior_period", "average_sales_price_last_year")

  val geographicSalesNumeric: Seq[String] = Seq("shipped_revenue", "shipped_revenue_percentage_total",
    "shipped_revenue_priod_period", "shipped_revenue_last_year", "shipped_units","shipped_units_percentage_total",
    "shipped_units_priod_period", "shipped_units_last_year", "average_sales_price",
    "average_sales_price_prior_period", "average_sales_price_last_year")

  val alternatePurchaseClean: Seq[String] = Seq("asin", "product_title", "first_purchase_asin",
    "first_purchase_product_title", "first_purchase_percentage", "second_purchase_asin", "second_purchase_product_title",
    "second_purchase_percentage", "third_purchase_asin", "third_purchase_product_title", "third_purchase_percentage",
    "fourth_purchase_asin", "fourth_purchase_product_title", "fourth_purchase_percentage",
    "fifth_purchase_asin", "fifth_purchase_product_title", "fifth_purchase_percentage")

  val alternatePurchaseNumeric: Seq[String] = Seq("first_purchase_percentage", "second_purchase_percentage",
    "third_purchase_percentage", "fourth_purchase_percentage", "fifth_purchase_percentage")

  val alternatePurchaseTransposeTransBy: Seq[String] = Seq("asin", "product_title", "start_date", "month", "quarter", "year", "day", "end_date")
  val alternatePurchaseTransposeTransOf: Seq[String] = Seq("purchase_product_title", "purchase_asin","purchase_percentage")

  val itemComparisonClean: Seq[String] = Seq("asin", "product_title", "first_compared_asin",
    "first_compared_product_title", "first_compared_pct", "second_compared_asin", "second_compared_product_title",
    "second_compared_pct", "third_compared_asin", "third_compared_product_title", "third_compared_pct",
    "fourth_compared_asin", "fourth_compared_product_title", "fourth_compared_pct",
    "fifth_compared_asin", "fifth_compared_product_title", "fifth_compared_pct")

  val itemComparisonNumeric: Seq[String] = Seq("first_compared_pct", "second_compared_pct",
    "third_compared_pct", "fourth_compared_pct", "fifth_compared_pct")

  val itemComparisonTransBy: Seq[String] =  Seq("asin", "product_title",  "start_date", "month", "quarter", "year", "day", "end_date")
  val itemComparisonTransOf: Seq[String] =  Seq("compared_asin", "compared_product_title","compared_pct")

  val amzTermsClean: Seq[String] = Seq("department", "search_term", "first_clicked_asin",
    "first_product_title", "first_click_share", "first_conversion_share", "second_clicked_asin",
    "second_product_title", "second_click_share", "second_conversion_share", "third_clicked_asin",
    "third_product_title", "third_click_share", "third_conversion_share")

  val amzTermsNumeric: Seq[String] = Seq("search_frequency_rank", "first_click_share", "first_conversion_share",
    "second_click_share", "second_conversion_share", "third_click_share", "third_conversion_share")

  val amzTermsTransposeTransBy: Seq[String] = Seq("department", "search_term", "search_frequency_rank", "start_date",
    "month", "quarter", "year", "day", "end_date")
  val amzTermsTransposeTransOf: Seq[String] =Seq("clicked_asin", "product_title","click_share", "conversion_share")

  val customerReviewsClean: Seq[String] = Seq("asin", "parent_asin", "product_title",
    "brand", "subcategory", "category", "avg_customer_rating", "avg_customer_rating_prior_period",
    "avg_customer_rating_life_to_date")

  val customerReviewsNumeric: Seq[String] = Seq("num_customer_reviews", "num_customer_reviews_prior_period",
    "num_customer_reviews_life_to_date", "avg_customer_rating", "avg_customer_rating_prior_period",
    "avg_customer_rating_life_to_date", "five_stars", "four_stars", "three_stars", "two_stars", "one_star")

  val forecastInventoryClean: Seq[String] = Seq("asin","parent_asin","product_title", "brand", "subcategory", "category",
    "rep_oos", "rep_oos_pct_total", "rep_oos_prior_period", "ordered_units_prior_period", "available_inventory",
    "available_inventory_prior_period", "open_purchase_order_quantity", "open_purchase_order_quantity_prior_period",
    "receive_fill_rate_pct", "overall_vendor_lead_time_in_days", "replenishment_category")

  val forecastOrderedInventoryTranspose: Seq[String] =Seq("asin", "parent_asin","ean","upc",
    "product_title","brand","subcategory","category","rep_oos","rep_oos_pct_total","rep_oos_prior_period",
    "ordered_units","ordered_units_prior_period","unfilled_customer_ordered_units","available_inventory",
    "available_inventory_prior_period","weeks_on_hand","open_purchase_order_quantity","open_purchase_order_quantity_prior_period",
    "receive_fill_rate_pct","overall_vendor_lead_time_in_days","replenishment_category", "start_date" , "month", "quarter", "year", "day", "end_date", "week", "week_amz")

  val forecastInventoryNumeric: Seq[String] = Seq("rep_oos", "rep_oos_pct_total", "rep_oos_prior_period", "ordered_units",
    "ordered_units_prior_period", "unfilled_customer_ordered_units","available_inventory", "available_inventory_prior_period",
    "weeks_on_hand", "open_purchase_order_quantity", "open_purchase_order_quantity_prior_period", "receive_fill_rate_pct",
    "overall_vendor_lead_time_in_days", "week_13_ordered_units","week_12_ordered_units","week_11_ordered_units",
    "week_10_ordered_units","week_09_ordered_units","week_08_ordered_units","week_07_ordered_units","week_06_ordered_units",
    "week_05_ordered_units","week_04_ordered_units","week_03_ordered_units","week_02_ordered_units","week_01_ordered_units",
    "week_01_mean_forecast","week_02_mean_forecast","week_03_mean_forecast","week_04_mean_forecast","week_05_mean_forecast",
    "week_06_mean_forecast","week_07_mean_forecast","week_08_mean_forecast","week_09_mean_forecast","week_10_mean_forecast",
    "week_11_mean_forecast","week_12_mean_forecast","week_13_mean_forecast","week_14_mean_forecast","week_15_mean_forecast",
    "week_16_mean_forecast","week_17_mean_forecast","week_18_mean_forecast","week_19_mean_forecast","week_20_mean_forecast",
    "week_21_mean_forecast","week_22_mean_forecast","week_23_mean_forecast","week_24_mean_forecast","week_25_mean_forecast",
    "week_26_mean_forecast","week_01_p70_forecast","week_02_p70_forecast","week_03_p70_forecast","week_04_p70_forecast",
    "week_05_p70_forecast","week_06_p70_forecast","week_07_p70_forecast","week_08_p70_forecast","week_09_p70_forecast",
    "week_10_p70_forecast","week_11_p70_forecast","week_12_p70_forecast","week_13_p70_forecast","week_14_p70_forecast",
    "week_15_p70_forecast","week_16_p70_forecast","week_17_p70_forecast","week_18_p70_forecast","week_19_p70_forecast",
    "week_20_p70_forecast","week_21_p70_forecast","week_22_p70_forecast","week_23_p70_forecast","week_24_p70_forecast",
    "week_25_p70_forecast","week_26_p70_forecast","week_01_p80_forecast","week_02_p80_forecast","week_03_p80_forecast",
    "week_04_p80_forecast","week_05_p80_forecast","week_06_p80_forecast","week_07_p80_forecast","week_08_p80_forecast",
    "week_09_p80_forecast","week_10_p80_forecast","week_11_p80_forecast","week_12_p80_forecast","week_13_p80_forecast",
    "week_14_p80_forecast","week_15_p80_forecast","week_16_p80_forecast","week_17_p80_forecast","week_18_p80_forecast",
    "week_19_p80_forecast","week_20_p80_forecast","week_21_p80_forecast","week_22_p80_forecast","week_23_p80_forecast",
    "week_24_p80_forecast","week_25_p80_forecast","week_26_p80_forecast","week_01_p90_forecast","week_02_p90_forecast",
    "week_03_p90_forecast","week_04_p90_forecast","week_05_p90_forecast","week_06_p90_forecast","week_07_p90_forecast",
    "week_08_p90_forecast","week_09_p90_forecast","week_10_p90_forecast","week_11_p90_forecast","week_12_p90_forecast",
    "week_13_p90_forecast","week_14_p90_forecast","week_15_p90_forecast","week_16_p90_forecast","week_17_p90_forecast",
    "week_18_p90_forecast","week_19_p90_forecast","week_20_p90_forecast","week_21_p90_forecast","week_22_p90_forecast",
    "week_23_p90_forecast","week_24_p90_forecast","week_25_p90_forecast","week_26_p90_forecast")

  val forecastShippedInventoryClean: Seq[String] = Seq("asin","product_title", "brand", "subcategory", "category",
    "rep_oos", "rep_oos_pct_total", "rep_oos_prior_period", "shipped_units_prior_period", "available_inventory",
    "available_inventory_prior_period", "open_purchase_order_quantity", "open_purchase_order_quantity_prior_period",
    "receive_fill_rate_pct", "overall_vendor_lead_time_in_days", "replenishment_category")

  val forecastShippedInventoryTranspose: Seq[String] =Seq("asin","parent_asin","ean","upc",
    "product_title","brand","subcategory","category","rep_oos","rep_oos_pct_total","rep_oos_prior_period",
    "shipped_units","shipped_units_prior_period","unfilled_customer_ordered_units","available_inventory",
    "available_inventory_prior_period","weeks_on_hand","open_purchase_order_quantity","open_purchase_order_quantity_prior_period",
    "receive_fill_rate_pct","overall_vendor_lead_time_in_days","replenishment_category", "start_date" , "month", "quarter","year", "day", "end_date", "week", "week_amz")

  val forecastShippedInventoryNumeric: Seq[String] = Seq("rep_oos", "rep_oos_pct_total", "rep_oos_prior_period", "shipped_units",
    "shipped_units_prior_period", "unfilled_customer_ordered_units", "available_inventory", "available_inventory_prior_period",
    "weeks_on_hand","open_purchase_order_quantity", "open_purchase_order_quantity_prior_period", "receive_fill_rate_pct",
    "overall_vendor_lead_time_in_days",
    "week_01_mean_forecast","week_02_mean_forecast","week_03_mean_forecast","week_04_mean_forecast","week_05_mean_forecast",
    "week_06_mean_forecast","week_07_mean_forecast","week_08_mean_forecast","week_09_mean_forecast","week_10_mean_forecast",
    "week_11_mean_forecast","week_12_mean_forecast","week_13_mean_forecast","week_14_mean_forecast","week_15_mean_forecast",
    "week_16_mean_forecast","week_17_mean_forecast","week_18_mean_forecast","week_19_mean_forecast","week_20_mean_forecast",
    "week_21_mean_forecast","week_22_mean_forecast","week_23_mean_forecast","week_24_mean_forecast","week_25_mean_forecast",
    "week_26_mean_forecast","week_01_p70_forecast","week_02_p70_forecast","week_03_p70_forecast","week_04_p70_forecast",
    "week_05_p70_forecast","week_06_p70_forecast","week_07_p70_forecast","week_08_p70_forecast","week_09_p70_forecast",
    "week_10_p70_forecast","week_11_p70_forecast","week_12_p70_forecast","week_13_p70_forecast","week_14_p70_forecast",
    "week_15_p70_forecast","week_16_p70_forecast","week_17_p70_forecast","week_18_p70_forecast","week_19_p70_forecast",
    "week_20_p70_forecast","week_21_p70_forecast","week_22_p70_forecast","week_23_p70_forecast","week_24_p70_forecast",
    "week_25_p70_forecast","week_26_p70_forecast","week_01_p80_forecast","week_02_p80_forecast","week_03_p80_forecast",
    "week_04_p80_forecast","week_05_p80_forecast","week_06_p80_forecast","week_07_p80_forecast","week_08_p80_forecast",
    "week_09_p80_forecast","week_10_p80_forecast","week_11_p80_forecast","week_12_p80_forecast","week_13_p80_forecast",
    "week_14_p80_forecast","week_15_p80_forecast","week_16_p80_forecast","week_17_p80_forecast","week_18_p80_forecast",
    "week_19_p80_forecast","week_20_p80_forecast","week_21_p80_forecast","week_22_p80_forecast","week_23_p80_forecast",
    "week_24_p80_forecast","week_25_p80_forecast","week_26_p80_forecast","week_01_p90_forecast","week_02_p90_forecast",
    "week_03_p90_forecast","week_04_p90_forecast","week_05_p90_forecast","week_06_p90_forecast","week_07_p90_forecast",
    "week_08_p90_forecast","week_09_p90_forecast","week_10_p90_forecast","week_11_p90_forecast","week_12_p90_forecast",
    "week_13_p90_forecast","week_14_p90_forecast","week_15_p90_forecast","week_16_p90_forecast","week_17_p90_forecast",
    "week_18_p90_forecast","week_19_p90_forecast","week_20_p90_forecast","week_21_p90_forecast","week_22_p90_forecast",
    "week_23_p90_forecast","week_24_p90_forecast","week_25_p90_forecast","week_26_p90_forecast")

  val inventoryHealthClean: Seq[String] = Seq("asin", "parent_asin", "product_title",
    "brand", "subcategory", "category", "net_received", "net_received_units", "sell_through_rate", "open_purchase_order_quantity",
    "sellable_on_hand_inventory", "sellable_on_hand_inventory_trailing_30_day_avg",
    "unsellable_on_hand_inventory", "unsellable_on_hand_inventory_trailing_30_day_avg", "aged_90_plus_days_sellable_inventory",
    "aged_90_plus_days_sellable_inventory_trailing_30_day_avg", "unhealthy_inventory", "unhealthy_inventory_trailing_30_day_avg",
    "aged_90_plus_days_sellable_units")

  val inventoryHealthNumeric: Seq[String] = Seq("net_received", "net_received_units", "sell_through_rate", "open_purchase_order_quantity",
    "sellable_on_hand_inventory", "sellable_on_hand_inventory_trailing_30_day_avg", "sellable_on_hand_units", "unsellable_on_hand_inventory",
    "unsellable_on_hand_inventory_trailing_30_day_avg", "unsellable_on_hand_units","aged_90_plus_days_sellable_inventory", "aged_90_plus_days_sellable_inventory_trailing_30_day_avg",
    "unhealthy_inventory", "unhealthy_inventory_trailing_30_day_avg", "unhealthy_units", "aged_90_plus_days_sellable_units")

  val marketBasketAnalysisClean: Seq[String] = Seq("asin", "product_title", "first_purchased_asin",
    "first_purchased_title", "first_combination_pct", "second_purchased_asin", "second_purchased_title",
    "second_combination_pct", "third_purchased_asin", "third_purchased_title", "third_combination_pct")

  val marketBasketAnalysisNumeric: Seq[String] = Seq("first_combination_pct", "second_combination_pct",
    "third_combination_pct")

  val marketBasketAnalysisTransBy: Seq[String] = Seq("asin", "product_title",  "start_date","month", "quarter", "year", "day","end_date")
  val marketBasketAnalysisTransOf: Seq[String] = Seq("purchased_asin", "purchased_title","combination_pct")

  val productCatalogClean: Seq[String] = Seq("asin","ean", "upc", "parent_asin", "product_title")

}
