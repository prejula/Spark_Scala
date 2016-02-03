package com.prej.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object SparkSqlHiveQuery {
  
  def main(args : Array[String])
  {
    val sparkConf = new SparkConf().setAppName("SparkSqlHiveQuery");
    val sc = new SparkContext(sparkConf);
    val hiveSqlContext = new HiveContext(sc);
    
    /*hiveSqlContext.sql("SET hive.input.dir.recursive=true");
    hiveSqlContext.sql("SET mapred.input.dir.recursive=true");
    hiveSqlContext.sql("SET hive.mapred.supports.subdirectories=true");
    hiveSqlContext.sql("SET mapreduce.input.fileinputformat.input.dir.recursive=true");*/
    hiveSqlContext.sql("SET hive.support.sql11.reserved.keywords=false");
    hiveSqlContext.sql("create table ATab(col1 INT, col2 String, col3 INT) ");
    hiveSqlContext.sql("load data local inpath \"/home/ubuntu/spark_examples/a.txt\" into table ATab");
    
    val results = hiveSqlContext.sql("select * from ATab");
    results.show();    
  }
}  