package com.beamsuntory.amz.de.commons

import com.beamsuntory.bgc.commons.Models

class UtilsTest extends UtilsAmzDE {

    def setConfigFiles: Unit = {
    spark.sqlContext.sparkContext.hadoopConfiguration.set("fs.gs.project.id", "test")
  }

//  override def createTableWithCaseClass(table: String , path: String, separator: String, model: Models): Unit  = {
//
//   // val df = createDataFrame(table,path, separator, model)
//
//    // Get dataset from dataframe using the table corresponding Case Class
//    //df.createOrReplaceTempView(table)
//  }

}
