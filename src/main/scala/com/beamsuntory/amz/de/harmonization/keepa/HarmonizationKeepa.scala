package com.beamsuntory.amz.de.harmonization.keepa


import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import com.beamsuntory.amz.de.commons.UtilsAmzDE
import com.beamsuntory.bgc.commons.DataFrameUtils._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import java.util.Calendar
import org.apache.spark.sql.DataFrame

class HarmonizationKeepa(val utils: UtilsAmzDE) {

  def mainExecution(): Unit = {
    harmonizationKeepaCleaning
  }

  def harmonizationKeepaCleaning(): Unit = {

    val now = Calendar.getInstance()
    val year_window = now.get(Calendar.YEAR) - 1

    val keepa_raw = utils.spark.read.table(amzDEraw + "keepa_pricing")
      .withColumn("data_type", when(col("data_type") === "LIGHTING_DEAL", lit("LIGHTNING_DEAL"))
        .otherwise(col("data_type")))

    val keepa_pricing_reviews = keepa_raw
      .filter(col("data_type") =!= "LIGHTNING_DEAL")
      .groupBy("asin", "data_type", "day", "month", "year", "title")
      .agg(max("timestamp").as("timestamp"),
        max("data_value").as("data_value"))

    val window_dates = Window.partitionBy("asin", "data_type").orderBy("timestamp")

    val keepa_pricing_lightning = harmonizationLightningDeal(keepa_raw)

    val keepa_pricing_mixing_dates = keepa_pricing_reviews
      .withColumn("diff_dates", datediff(lead(col("timestamp"), 1).over(window_dates), col("timestamp")))
      .filter(col("diff_dates") > 1)
      .withColumn("mixing_dates", create_mixing_dates(col("timestamp").cast("string"), col("diff_dates")))
      .withColumn("timestamp", explode(col("mixing_dates")))
      .withColumn("timestamp", col("timestamp").cast(TimestampType))
      .withColumn("data_value", lit(null))

    val window_last_value = Window.partitionBy("asin", "data_type").orderBy("timestamp")
      .rowsBetween(Window.unboundedPreceding, -1)

    val keepa_full_dates = keepa_pricing_reviews
      .fullUnion(keepa_pricing_mixing_dates).orderBy("data_type", "asin", "timestamp")
      .withColumn("data_value", coalesce(col("data_value"),
        last(col("data_value"), true).over(window_last_value)))

    keepa_full_dates.fullUnion(keepa_pricing_lightning.filter(col("data_value").isNotNull))
      .filter(col("year") >= year_window)
      .withColumn("week", weekofyear(col("timestamp")).cast("integer"))
      .withColumn("year", year(col("timestamp")))
      .withColumn("month", month(col("timestamp")))
      .withColumn("day", dayofmonth(col("timestamp")))
      .writeHiveTable(amzDEhar + "keepa_pricing_full_dates")
  }

  def harmonizationLightningDeal(df_raw_keepa: DataFrame): DataFrame = {

    val window_dates_light = Window.partitionBy("asin", "data_type").orderBy("timestamp")

    df_raw_keepa
      .filter(col("data_type") === "LIGHTNING_DEAL")
      .withColumn("diff_dates_seconds", unix_timestamp(lead(col("timestamp"), 1).over(window_dates_light)) - unix_timestamp(col("timestamp")))
      .withColumn("duration_hours", round(col("diff_dates_seconds") / 3600).cast("int"))
      .withColumn("duration_minutes", round((col("diff_dates_seconds") - col("duration_hours") * 3600) / 60).cast("int"))
      .withColumn("duration_seconds", round((col("diff_dates_seconds")
        - col("duration_hours") * 3600 + col("duration_minutes") * 60) / 60).cast("int"))
      .withColumn("duration", concat_ws(":", col("duration_hours"), col("duration_minutes"), col("duration_seconds")))
      .filter(col("data_value").isNotNull or col("data_value") =!= -1)
  }

  //Create array column with mixing gap dates
  val create_mixing_dates : UserDefinedFunction = udf((start: String, excludedDiff: Int) => {
    val dtFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
    val fromDt = LocalDateTime.parse(start.substring(0,16), dtFormatter)
    (1 to (excludedDiff - 1)).map(day => {
      val dt = fromDt.plusDays(day)
      "%4d-%2d-%2d".format(dt.getYear, dt.getMonthValue, dt.getDayOfMonth)
        .replaceAll(" ", "0")
    })
  })

}