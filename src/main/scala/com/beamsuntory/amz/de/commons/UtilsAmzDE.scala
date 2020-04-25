package com.beamsuntory.amz.de.commons

import com.beamsuntory.bgc.commons.Utils
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.UserDefinedFunction
import java.text.SimpleDateFormat
import java.sql.Date
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class UtilsAmzDE extends Utils {

  val logger: Logger = Logger.getLogger(getClass.getName)
  val rootConfig = "amazon"
  val defaultFile = "application.conf"
  val config: Config = ConfigFactory.parseResources(defaultFile).getConfig(rootConfig).resolve()
  val message: String = config.root().render()

  logger.info(s"New configuration created : {$message}")

  val deleteNonUsedChars: UserDefinedFunction = udf { str: String =>
    str match {
      case null => null
      case "UNKNOWN" => null
      case "—" => null
      case "" => null
      case n if n.matches("€.+") => n.replaceAll("€", "")
      case n if n.matches(".+%") => n.replaceAll("%", "")
      case _ => str
    }
  }

  val castToNumeric: UserDefinedFunction = udf {str: String =>
    str match{
      case null => null
      case n if n.contains(".") => n.replaceAll(",", "")
      case n if n.contains(",") => n.replaceAll(",", "")
      case _ => str
    }
  }

  val calculateWeekAmazon: UserDefinedFunction = udf {(date: Date, week: Int) =>
    val dowInt = new SimpleDateFormat("u")
    val numb = dowInt.format(date)
    numb match {
      case "7" => week + 1
      case _ => week
    }
  }

  val mappingMonthNameUDF: UserDefinedFunction = udf { (month: Int) =>
    month match {
      case 1 => "January"
      case 2 => "February"
      case 3 => "March"
      case 4 => "April"
      case 5 => "May"
      case 6 => "June"
      case 7 => "July"
      case 8 => "August"
      case 9 => "September"
      case 10 => "October"
      case 11 => "November"
      case 12 => "December"
      case _ => ""
    }
  }

  val mappingMonthGermanyNumberUDF: UserDefinedFunction = udf { (month: String) =>
    month match {
      case "Januar" => 1
      case "Februar" => 2
      case "März" => 3
      case "April" => 4
      case "Mai" => 5
      case "Juni" => 6
      case "Juli" => 7
      case "August" => 8
      case "September" => 9
      case "Oktober" => 10
      case "November" => 11
      case "Dezember" => 12
      case _ => 0
    }
  }

    val mapping_hierarchy_campaigns_hsa: UserDefinedFunction = udf { (items: String) =>
      items match {
        case "Larios" => "GIN-LARIOS FAMILY-LARIOS_BRAND-BEAM"
        case "Ardmore" => "SCOTCH-ARDMORE FAMILY-ARDMORE BRAND-BEAM"
        case "Bowmore" => "SCOTCH-BOWMORE FAMILY-BOWMORE BRAND-SUNTORY"
        case "Sipsmith" => "GIN-SIPSMITH GIN FAMILY-SIPSMITH GIN BRAND-BEAM"
        case "Connemara" => "IRISH WHISKEY-COOLEYS FAMILY-CONNEMARA BRAND-BEAM"
        case "Laphroaig" => "SCOTCH-LAPHROAIG FAMILY-LAPHROAIG BRAND-BEAM"
        case "TheGlenrothes" => "SCOTCH-GLENROTHES FAMILY-GLENROTHES BRAND-THIRD PARTY"
        case "Makers" => "BOURBON-MAKERS MARK FAMILY-MAKERS MARK BRAND-BEAM"
        case _ => "MIXED-MIXED FAMILIES-MIXED BRANDS-OTHERS"
      }
    }

    val mapping_cities_geographic: UserDefinedFunction = udf { (str: String) =>
      str match {
        case n if n.contains("BERLIN") => "BERLIN"
        case n if n.contains("MÜNCHEN") => "MÜNCHEN"
        case n if n.contains("HAMBURG") => "HAMBURG"
        case n if n.contains("FRANKFURT") => "FRANKFURT"
        case n if n.contains("DÜSSELDORF") => "DÜSSELDORF"
        case n if n.contains("STUTTGART") => "STUTTGART"
        case n if n.contains("KÖLN") => "KÖLN"
        case _ => "OTHERS"
      }
    }

    def applyUdfRecursiverly(df: DataFrame, udf: UserDefinedFunction, colname: String): DataFrame = {
      df.withColumn(colname, udf(col(colname)))
    }

    def applyUdfRecursiverly(df: DataFrame, udf: UserDefinedFunction, colnames: String*): DataFrame = {
      if (colnames.isEmpty) df
      else applyUdfRecursiverly(applyUdfRecursiverly(df, udf, colnames.head), udf, colnames.tail: _*)
    }

    def createWeekOfYear(df: DataFrame): DataFrame = {
      df.withColumn("week", weekofyear(col("start_date")).cast("int"))
        .withColumn("week_amz", calculateWeekAmazon(col("start_date"), col("week")))
    }

    def transposeUDF(transDF: DataFrame, transBy: Seq[String]): DataFrame = {
      val (cols, types) = transDF.dtypes.filter { case (c, _) => !transBy.contains(c) }.unzip
      require(types.distinct.size == 1)

      val kvs = explode(array(
        cols.map(c => struct(lit(c).alias("week_model"), col(c).alias("value_model"))): _*
      ))

      val byExprs = transBy.map(col(_))

      transDF
        .select(byExprs :+ kvs.alias("_kvs"): _*)
        .select(byExprs ++ Seq(col("_kvs.week_model"), col("_kvs.value_model")): _*)
    }

    /*
  Applies coalesce to both column with same name from current and last dataframes
  @param name String field name
  */
    def chooseField(name: String): Column = {
      coalesce(col("ord." + name), col("shi." + name)).alias(name)
    }

    /*
  Applies coalesce to both column with same name from current and last dataframes
  @param name String field name
  @param alias String alias
  */
    def chooseField(name: String, alias: String): Column = {
      coalesce(col("ord." + name), col("shi." + name)).alias(alias)
    }

    def createAuxColumns(df: DataFrame, colname: String): DataFrame = {
      df.withColumn(colname + "_aux", col(colname))
    }

    def createAuxColumns(df: DataFrame, allcol: String*): DataFrame = {
      if (allcol.isEmpty) df
      else createAuxColumns(createAuxColumns(df, allcol.head), allcol.tail: _*)
    }

    def singleSpace(col: Column): Column = {
      trim(regexp_replace(col, " +", " "))
    }

    def coalescenceGLColumns(df: DataFrame, colname: String): DataFrame = {
      df.withColumn(colname, coalesce(col(colname), lit("")))
    }

    def coalescenceGLColumns(df: DataFrame, allcol: String*): DataFrame = {
      if (allcol.isEmpty) df
      else coalescenceGLColumns(coalescenceGLColumns(df, allcol.head), allcol.tail: _*)
    }

    def loadGLCustomer(df: DataFrame): DataFrame = {

      val columnsCustomer: Seq[String] = Seq("cust_sales", "cust_sales_description", "sales_dist", "sales_dist_description",
        "country", "zzcusth1", "zzcusth1_description", "zzcusth2", "zzcusth2_description",
        "zzcusth3", "zzcusth3_description", "zzcusth3", "zzcusth3_description",
        "zzcusth4", "zzcusth4_description", "zzcusth5", "zzcusth5_description",
        "zzcusth6", "zzcusth6_description", "zzcusth7", "zzcusth7_description")

      coalescenceGLColumns(df.filter(col("cust_sales") =!= "CUST_SALES"), columnsCustomer: _*)

    }

  val date_transform: UserDefinedFunction = udf((date: String) => {
    val dtFormatter = DateTimeFormatter.ofPattern("y-M-d")
    val dt = LocalDate.parse(date, dtFormatter)
    "%4d-%2d-%2d".format(dt.getYear, dt.getMonthValue, dt.getDayOfMonth)
      .replaceAll(" ", "0")
  })

  def fillwithUnassigned(df: DataFrame): DataFrame = {
    df
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
      .filter(col("brand_partner") =!= "DELETED")
  }
}
