# Databricks notebook source
# MAGIC %md
# MAGIC ##### Data Ingestion of Result #####

# COMMAND ----------

dbutils.widgets.text("p_Filedate","2021-03-21")
V_Filedate = dbutils.widgets.get("p_Filedate")

dbutils.widgets.text("P_source","Manual")
V_Source = dbutils.widgets.get("P_source")

# COMMAND ----------

# DBTITLE 1,Import Functions 
# MAGIC %run /Users/Saravana_admin@5njbxz.onmicrosoft.com/Include/Config_File

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
         .json(f"/mnt/raw/{V_Filedate}/results.json")


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
                                .withColumn("Field_date",lit(V_Filedate))\
                                .withColumn("Source",lit(V_Source))\
                                .drop(col("statusId"))


# COMMAND ----------

#Results_Transform_df.write.mode("overwrite").parquet("dbfs:/mnt/processed/results")

# COMMAND ----------

def merge_delta_data(input_df,db_name,table_name,filepath,Merge_condtion,Partition_column):
    from delta.tables import DeltaTable
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
           TargetTbl = DeltaTable.forPath(spark, f"{filepath}/{table_name}")
           TargetTbl.alias("tgt").merge(input_df.alias("src"),{Merge_condtion})\
                    .whenMatchedUpdateAll()\
                    .whenNotMatchedInsertAll()\
                    .execute()
    else:
         input_df.write.mode("overwrite").partitionBy(Partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

Merge_condition = "tgt.result_id= src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(Results_Transform_df,'f1_processed','results',processed_folder_path,Merge_condition,'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1) from f1_processed.results
# MAGIC Group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

# MAGIC %sql show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from f1_processed.results

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc extended f1_processed.results

# COMMAND ----------

