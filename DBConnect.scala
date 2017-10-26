package com.mindtree.sparkstreaming

import org.apache.spark.sql.SparkSession

object DBConnect {
  
  def main(args: Array[String]) {
  
      val spark = SparkSession
      .builder
      .appName("DB Connect")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
      
      val df = spark.read.format("jdbc").
      option("url", "jdbc:mysql://localhost:3306/library_app").
      option("dbtable", "book").
      option("driver", "com.mysql.jdbc.Driver").
      option("user", "root").
      option("password", "root").
      load();
      
      df.show()
      
      val df201 = spark.read.format("com.databricks.spark.xml")
                  .option("inferSchema", true)
                  .option("rootTag","Hotels")
                  .option("rowTag", "Hotel")
                  .load("C:\\Users\\Pshivapu\\Desktop\\SparkScala\\Mindtree201\\Exercise_1.xml")
      df201.show()
   }
}