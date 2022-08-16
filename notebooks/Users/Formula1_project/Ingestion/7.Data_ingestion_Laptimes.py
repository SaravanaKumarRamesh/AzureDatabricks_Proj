# Databricks notebook source
# MAGIC %md
# MAGIC ##### Data Ingestion of laptimes #####

# COMMAND ----------

# DBTITLE 1,Import Functions 
from pyspark.sql.types import IntegerType,StructType,StructField,StringType
from pyspark.sql.functions import *

# COMMAND ----------

laptimes_Schema = StructType(fields = [ StructField ("raceId",IntegerType(),False),
                                        StructField ("driverId",IntegerType(),False),
                                        StructField ("lap",IntegerType(),False),
                                        StructField ("position",IntegerType(),False),
                                        StructField ("time",StringType(),False),
                                        StructField ("milliseconds",IntegerType(),False)
                                    
])

laptimes_Df = spark.read\
            .schema(laptimes_Schema)\
            .csv("/mnt/raw/lap_times")

# COMMAND ----------

Laptimes_transform_df = laptimes_Df.withColumnRenamed("raceId","race_Id")\
                                .withColumnRenamed("driverId","driver_Id")\
                                .withColumn("Ingestion_time",current_timestamp())

# COMMAND ----------

Laptimes_transform_df.write.mode("overwrite").parquet("dbfs:/mnt/processed/lap_times")

# COMMAND ----------

display(Laptimes_transform_df)

# COMMAND ----------

