// Databricks notebook source
// MAGIC %md # Scala RDD implementation with accumulators

// COMMAND ----------

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf 
import scala.io.Source
import sys.process._
import java.io._
import sys.env
sc

// COMMAND ----------

// load and preprocess the data
def preprocess_graph_data(path:String) : RDD[(Int, Int)] = {
  return sc.textFile(path)
           .filter(x => !(x.contains("#")))
           .map(_.split("\t"))
           .map(x => (x(0).toInt, x(1).toInt))
}

// COMMAND ----------

// map 
def ccf_iterate_map(key:Int, value:Int) = Array((key, value), (value, key))

// COMMAND ----------

var counter = sc.longAccumulator("global_counter")

def ccf_iterate_reduce_acc(key:Int, values:Array[Int]) = {
  
  var min_key = key
  var result = new ArrayBuffer[(Int, Int)]() // we use ArrayBuffer because it is mutable
  var value = 0

  for(value <- values){
    if(value < min_key){
      min_key = value
    }
  } // this loop finds the minimum value
  
  if(min_key < key){
    result += ((key, min_key))
    for(value <- values){
      if(min_key != value){
        counter.add(1) // counter is an accumulator
        result += ((value, min_key))
      }
    }
  }
  (result.toArray, counter)
}

// COMMAND ----------

// variables initialization
var data_result = preprocess_graph_data("/FileStore/tables/web_Google.txt")
counter.setValue(1)
var iteration = 0

// iterations
while(counter.value != 0){
  
  iteration += 1
  counter.reset()
  
  // map phase
  var data_iterate = data_result.flatMap(x => ccf_iterate_map(x._1, x._2))
  var data_grouped = data_iterate.groupByKey().mapValues(_.toArray)
  
  // reduce phase
  var data_reduced = data_grouped.map(x => ccf_iterate_reduce_acc(x._1.toInt, x._2))
  data_result = data_reduced.flatMap(x => x._1).distinct
  data_result.count()
  
  println("Iteration no. " + iteration + ": " + counter.value + " new pairs created")
}

val n_components = data_result.map(_._2).distinct().count()
println("Number of components: " + n_components) // 4.33 mn

// COMMAND ----------

// MAGIC %md ### Same algorithm with deduplication (section 2.6 of the report)

// COMMAND ----------

// reduce function
def ccf_iterate(key:Int, values:ArrayBuffer[Int], accumulator:LongAccumulator) : ArrayBuffer[(Int, Int)] = {
  var min_value = key
  var value_list = new ArrayBuffer[Int]()
  var output = new ArrayBuffer[(Int, Int)]()
  
  for(value <- values) {
    if(value < min_value) {
      min_value = value
    }
    value_list += value
  }
  
  if(min_value < key) {
    output += ((key, min_value))
    
    for(value <- value_list) {
      if(min_value != value) {
         accumulator.add(1)
         output += ((value, min_value))
      }
    }
  }
  return output
}

// reduce function with secondary sorting
def ccf_iterate_reduce_secondary_sorting(key:Int, values:ArrayBuffer[Int], accumulator:LongAccumulator) : ArrayBuffer[(Int, Int)] = {
  var min_value = values.remove(0)
  var output = new ArrayBuffer[(Int, Int)]()
  
  if(min_value < key) {
    output += ((key, min_value))
    
    for(value <- values) {
      accumulator.add(1)
      output += ((value, min_value))
    }
  } 
  return output
}

// loading and preprocessing the data
val graph_data = preprocess_graph_data("/FileStore/tables/web_Google.txt")

// variables initialization
var accumulator = sc.longAccumulator("ccf_accumulator")
var res = graph_data
var i : Int = 1
  
accumulator.add(1)

// CCF algorithm
while(accumulator.value > 0) {
    
  accumulator.reset()
  println(s"====== Loop $i ======")
    
  // CCF-Iterate
  val data_mapped = res
        .flatMap(x => Array(Array(x._1, x._2), Array(x._2, x._1)))
        .map(x => (x(0), x(1)))
        .groupByKey()
        .mapValues(x => x.to[ArrayBuffer].sorted)
  
  println(data_mapped.count())
            
  val data_reduced = data_mapped.flatMap(
        x => ccf_iterate_reduce_secondary_sorting(x._1, x._2, accumulator)
  )
  
  println(data_reduced.count())
        
  // CCF-Dedup
  val data_dedup_mapped = data_reduced
        .map(x => ((x._1, x._2), None))
        .groupByKey()
  
  println(data_dedup_mapped.count())
    
  res = data_dedup_mapped.map(x => (x._1._1, x._1._2))
  
  i += 1
    
  val acc_val = accumulator.value
  println(s"Accumulator value: $acc_val")
}

// COMMAND ----------

// MAGIC %md # Scala DataFrame implementation

// COMMAND ----------

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import spark.implicits._
import org.apache.spark.util.LongAccumulator

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, DoubleType, IntegerType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.split

import scala.io.Source
import spark.sqlContext.implicits._
sc

// COMMAND ----------

// loading the data
val schema = StructType(List(StructField("key", IntegerType, true), StructField("value", IntegerType, true)))
var df = spark.read.format("csv").option("header", "false").option("delimiter", "\t").schema(schema).load("/FileStore/tables/web_Google-2.txt")

// preprocessing
df = df.na.drop() 

// COMMAND ----------

// reduce function - will be wrapped as a user-defined function
def iterate_reduce(key: Int, values: Seq[Int]): ListBuffer[(Int, Int, Int)] = {
  
    val list_values = values.to[ListBuffer]
    val min_value = list_values.remove(list_values.indexOf(list_values.min))
    var result = ListBuffer[(Int, Int, Int)]()
    if (min_value < key) {
      result += ((key, min_value, 0))
      for (i <- 0 until list_values.length) {
        result += ((list_values(i), min_value, 1))
      }
    }
    return result
  }

// COMMAND ----------

// preparing the algorithm
var counter = 1L
val iterate_reduce_ccf = udf {(x: Int, y: Seq[Int]) => iterate_reduce(x, y)}
var iteration = 1

while (counter != 0) {
  
  // map phase
  val mapped = df.select(col("value").alias("key"), col("key").alias("value"))
  df = df.union(mapped)
  df = df.groupBy("key").agg(collect_list("value").alias("value")) 
  
  // reduce phase
  df = df.withColumn("iterate_reduce_ccf", iterate_reduce_ccf(col("key"), col("value"))).select(col("iterate_reduce_ccf"))
  df = df.select(explode(col("iterate_reduce_ccf")))
  df = df.na.drop() // clear the dataframe
  df = df.select(col("col._1").alias("key"), col("col._2").alias("value"), col("col._3").alias("counter"))

  // deduplication phase
  df = df.dropDuplicates()
  
  // count the number of pairs created (to control the loop)
  counter = df.agg(sum("counter")).first.getLong(0)
  
  // prepare the result for the next iteration
  df = df.select(col("key"), col("value"))
  println("Iteration no. " + iteration + ": " + counter + "new pairs created")
  iteration += 1
} // 1.59 hour
