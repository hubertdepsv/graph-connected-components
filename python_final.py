# Databricks notebook source
# MAGIC %md # Python RDD implementation

# COMMAND ----------

# Data loading
data = sc.textFile('/FileStore/tables/web_Google-2.txt') # importing the data as an RDD

# Preprocessing
no_header_rdd = data.zipWithIndex().filter(lambda row_index: row_index[1] > 3).keys() # column names are FromeNodeId and ToNodeId
no_header_rdd = no_header_rdd.map(lambda x: (int(x.split("\t")[0]), int(x.split("\t")[1]))) # we split the data into 2 columns

# COMMAND ----------

# Map function
def iterate_map_v1(key, value):
  return (key, value), (value, key)

# COMMAND ----------

# Reduce function with accumulator (sorted values)
def iterate_reduce_sorted(key, values):

  values = sorted(values)  
  global counter
  result = []
  min_key = values[0]
  
  if min_key < key:
    result.append((key, min_key))
    for value in values:
      if min_key != value:
        counter.add(1)
        result.append((value, min_key))
        
  return result, counter

# COMMAND ----------

# Map and Reduce functions for the deduplication step
def ccf_map_dedup(key, value):
  return ((key, value), None)

def ccf_reduce_dedup(key, values):
  return key[0], key[1]

# COMMAND ----------

# CCF algorithm setup
data_result = no_header_rdd
dedup = True

# COMMAND ----------

# CCF algorithm
counter = sc.accumulator(1) # initialize the accumulator
iteration = 1

while (counter.value != 0):
  # Map
  data_iterate = data_result.flatMap(lambda x: iterate_map_v1(x[0], x[1]))
  data_grouped = data_iterate.groupByKey().mapValues(list) 
  # we group data_iterate by key, then we transform the result into a list
  
  counter.value = 0 # local accumulator initialization
  
  #Reduce
  data_reduced = data_grouped.map(lambda x: iterate_reduce_sorted(x[0], x[1]))
  data_result = data_reduced.flatMap(lambda x: x[0]).distinct()
  data_result.collect()
  
  print(f"Iteration no. {iteration}: {counter.value} new pairs created")
  iteration += 1
  
  if dedup: # deduplication
    data_dedup = data_result.flatMap(lambda x: ccf_map_dedup(x[0], x[1])) # map with dedup, similar to the above version
    data_grouped = data_dedup.groupByKey().mapValues(list)
    
    data_reduced = data_grouped.map(lambda x: ccf_reduce_dedup(x[0], x[1])) # reduce with dedup, similar to the above version
  
  data = data_result # input of the next iteration
  
nb_components = data.map(lambda x: x[1]).distinct().count()
print(f"Number of components: {nb_components}")

# COMMAND ----------

# MAGIC %md # Python DataFrame implementation

# COMMAND ----------

from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DataType, FloatType, MapType, Row
import pyspark.sql.functions as F
sc

# COMMAND ----------

# loading and preprocessing the data
schema = StructType([StructField("key", IntegerType(), True), StructField("value", IntegerType(), True)])

df = spark.read.format('csv').load('/FileStore/tables/web_Google-2.txt', headers=False, delimiter='\t', schema=schema)
df = df.na.drop() # to remove the empty header lines 

# COMMAND ----------

# reduce function
def iterate_reduce(key, values):
  
    result = {}
    min_value = values.pop(values.index(min(values)))
    result[key] = min_value
    
    if min_value < key:
      for value in values:
        counter.add(1)
        result[value] = min_value
        
    else:
      result = None
      
    return result
  
# we wrap it to yield a user-defined functions usable by .withColumn
iterate_reduce_ccf = F.udf(lambda x, y: iterate_reduce(x, y), MapType(IntegerType(), IntegerType()))

# COMMAND ----------

# CCF algorithm

counter = sc.accumulator(1)
iteration = 1

while counter.value != 0:
  
  # map phase
  mapped = df.select(F.col('value').alias('key'), F.col('key').alias('value'))
  df = df.union(mapped)
  df = df.groupBy('key').agg(F.collect_list('value').alias('value'))
  old_count = counter.value
  
  # reduce phase
  counter.value = 0 
  df = df.withColumn('iterate_reduce_ccf', iterate_reduce_ccf('key', 'value')).select('iterate_reduce_ccf')
  df.collect() # count?
  new_count = counter.value

  # explode & clear nones
  df = df.select(F.explode('iterate_reduce_ccf'))
  df = df.na.drop()

  # deduplication phase
  df = df.dropDuplicates()
  print(f'Iteration no. {iteration}: {abs(new_count - old_count)} new pairs created')
  iteration += 1 # 19.87 mn

# COMMAND ----------

nb_components = df.map(lambda x: x[1]).distinct().count()
print(f"Number of components: {nb_components}")
