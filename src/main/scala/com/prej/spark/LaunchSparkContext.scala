package com.prej.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object LaunchSparkContext {
  
  def main(args : Array[String])
  {
    val conf = new SparkConf().setAppName("LearnSpark")/*.setMaster("hdfs://localhost:54310")*/;
    val sc = new SparkContext(conf);
    println("Inside LaunchSparkContext");
    
    val textFile = sc.textFile("/home/ubuntu/spark_examples/a.txt");
    
    val filteredRecords = textFile.filter( line => line.contains("ppp") );
    
    println("no. of lines that contain ppp are : " + filteredRecords.count());
    
    val counts = textFile.flatMap(line => line.split(" "))
                .map(word => (word, 1))
                .reduceByKey(_ + _);
    println( "word count:: " + counts);  
    
    counts.saveAsTextFile("hdfs://localhost:54310/tmp/counts");
  }
}