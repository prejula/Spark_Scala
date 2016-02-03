package com.prej.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object SparkSqlSchemaMerging {
  
  def main(args : Array[String])
  {
    val sparkConf = new SparkConf().setAppName("SparkScehmaMerging");
    val sc = new SparkContext(sparkConf);
    val sqlContext = new SQLContext(sc);
    
    import sqlContext.implicits._;
    
    val df1 = sc.makeRDD(1 to 5).map(i => (i, i*2)).toDF("single", "double");
    df1.show();
    df1.write.parquet("/home/ubuntu/spark_examples/schema_merge/data=double");
    
    val df2 = sc.makeRDD(1 to 5).map(i => (i, i*3)).toDF("single", "triple");
    df2.show();
    df2.write.parquet("/home/ubuntu/spark_examples/schema_merge/data=triple");
    
    val df3 = sqlContext.read.option("mergeSchema", "true").parquet("/home/ubuntu/spark_examples/schema_merge");
    df3.printSchema();
    df3.registerTempTable("schema_merge");
    
    val df4 = sqlContext.sql("select * from schema_merge");
    df4.show(); 
  }
}