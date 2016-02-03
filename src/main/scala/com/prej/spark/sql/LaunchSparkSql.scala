package com.prej.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object LaunchSparkSql {
  
  def main (args : Array[String])
  {
    val conf = new SparkConf().setAppName("LearnSparkSql");
    val sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc);
    
    val df = sqlContext.read.format("json").load("/home/ubuntu/spark_examples/example.json");
    
    println("show all data");
    df.show();
    
    println("show schema");
    df.printSchema();
    
    println("show columns");
    df.columns(1);
    
    println("show name");
    df.select("name").show();   
    
    println("show age");
    df.select("age").show();
    
    println("show name and age with df");
    df.select(df("age") + 1, df("name")).show();
    
    println("select names with age greater than 20");
    df.filter(df("age") > 20).show();
    
    println("group by age");
    df.groupBy("age").count().show();
  }
}