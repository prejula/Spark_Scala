package com.prej.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructField

object SparkSqlSchema {
  
  def main (args : Array[String])
  {
    val sparkConf = new SparkConf().setAppName("SparkSchemaProgrammatically");
    val sc = new SparkContext(sparkConf);
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._;
    import org.apache.spark.sql.Row;
    
    val people = sc.textFile("/home/ubuntu/spark-1.5.1-bin-hadoop2.6/examples/src/main/resources/people.txt");
    val rowRdd = people.map(line => line.split(",")).map(p => Row(p(0), p(1).trim()));
    
    val schemaString = "name age";
    import org.apache.spark.sql.types.{StructType,StructField,StringType};
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)));
    
    val peopleFrame = sqlContext.createDataFrame(rowRdd, schema);
    peopleFrame.registerTempTable("people");
    
    val sqlResults = sqlContext.sql("select * from people");
    sqlResults.show();
    
    sqlResults.map(row => "Name is : " + row(0)).collect().foreach(println);    
  }
}