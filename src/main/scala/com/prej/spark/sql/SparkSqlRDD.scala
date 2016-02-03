package com.prej.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object SparkSqlRDD {
  
  def main (args : Array[String])
  {
    println("Inside SparkSqlRDD");
     
    val conf = new SparkConf().setAppName("LearnSpark");
    val sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._;

    val people = sc.textFile("/home/ubuntu/spark-1.5.1-bin-hadoop2.6/examples/src/main/resources/people.txt");
    val df = people.map(line => line.split(",")).map(p => Person(p(0), p(1).trim().toInt)).toDF();
    df.registerTempTable("people");
    
   // println("show df");
    df.show();
    
    val teenagers = sqlContext.sql("select * from people where age < 20 and age > 12");
    println("show teenagers");
    teenagers.show();
    
    println("print names");
    teenagers.map (t => "Name is : " + t(0)).collect().foreach (name => println(name));
    
    println("print ages");
    teenagers.map (t => "Age is : " + t(1)).collect().foreach (age => println(age));
    
    println("print names");
    teenagers.map (t => "Name is : " + t.getAs[String]("name")).collect().foreach(println);
    
    println("print map");
    teenagers.map (t =>  t.getValuesMap[Any](List("name", "age"))).collect().foreach(println);
    
   // teenagers.write.parquet("/home/ubuntu/spark-1.5.1-bin-hadoop2.6/examples/src/main/resources/teenagers.parquet");
    val teenParquet = sqlContext.read.parquet("/home/ubuntu/spark-1.5.1-bin-hadoop2.6/examples/src/main/resources/teenagers.parquet");
    teenParquet.registerTempTable("teenagers");

    val teenParquetDf = sqlContext.sql("select * from teenagers");
    
    println("show parquet teen data");
    teenParquetDf.show();
    
    println("show parquet teen schema");
    teenParquetDf.printSchema();
    
    println("show parquet teen names");
    teenParquetDf.map(teen => teen(0)).collect().foreach(println);
  }  
  
  case class Person(name: String, age: Int);
}