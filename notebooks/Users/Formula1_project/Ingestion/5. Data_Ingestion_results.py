# Databricks notebook source
# MAGIC %md
# MAGIC ##### Data Ingestion of Result #####

# COMMAND ----------

# DBTITLE 1,Import Functions 
from pyspark.sql.types import IntegerType,StringType,StructType,StructField,FloatType
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Create a schema and import data
Results_schema = StructType (fields = [StructField("resultId",IntegerType(),True),
                                        StructField("raceId",IntegerType(),True),
                                        StructField("driverId",IntegerType(),True),
                                        StructField("constructorId",IntegerType(),True),
                                        StructField("number",IntegerType(),True),
                                        StructField("grid",IntegerType(),True),
                                        StructField("position",IntegerType(),True),
                                        StructField("positionText",StringType(),True),
                                        StructField("positionOrder",IntegerType(),True),
                                        StructField("points",FloatType(),True),
                                        StructField("laps",IntegerType(),True),
                                        StructField("time",StringType(),True),
                                        StructField("milliseconds",IntegerType(),True),
                                        StructField("fastestLap",IntegerType(),True),
                                        StructField("rank",IntegerType(),True),
                                        StructField("fastestLapTime",StringType(),True),
                                        StructField("fastestLapSpeed",FloatType(),True),
                                        StructField("statusId",IntegerType(),True)
                                        ])

Results_df = spark.read\
         .schema(Results_schema)\
         .json("/mnt/raw/results.json")


# COMMAND ----------

display(Results_df)

# COMMAND ----------

# DBTITLE 1,Transform the data
Results_Transform_df = Results_df.withColumnRenamed("resultId","result_Id")\
                                .withColumnRenamed("raceId","race_Id")\
                                .withColumnRenamed("driverId","driver_Id")\
                                .withColumnRenamed("driverId","driver_Id")\
                                .withColumnRenamed("constructorId","constructor_Id")\
                                .withColumnRenamed("positionText","position_Text")\
                                .withColumnRenamed("positionOrder","position_Order")\
                                .withColumnRenamed("fastestLap","fastest_Lap")\
                                .withColumnRenamed("fastestLapTime","fastest_Lap_Time")\
                                .withColumnRenamed("fastestLapSpeed","fastest_Lap_Speed")\
                                .withColumn("Ingestion_time",current_timestamp())\
                                .drop(col("statusId"))


# COMMAND ----------

Results_Transform_df.write.mode("overwrite").parquet("dbfs:/mnt/processed/results")

# COMMAND ----------

