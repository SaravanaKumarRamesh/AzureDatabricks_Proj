# Databricks notebook source
# MAGIC %md
# MAGIC ##### Data Ingestion of Pitstops #####

# COMMAND ----------

dbutils.widgets.text("p_Filedate","2021-03-21")
V_Filedate = dbutils.widgets.get("p_Filedate")

dbutils.widgets.text("P_source","Manual")
V_Source = dbutils.widgets.get("P_source")

# COMMAND ----------

# MAGIC %run /Users/Saravana_admin@5njbxz.onmicrosoft.com/Include/Config_File

# COMMAND ----------

Pitstops_Schema = StructType(fields = [ StructField ("raceId",IntegerType(),False),
                                        StructField ("driverId",StringType(),False),
                                        StructField ("stop",IntegerType(),True),
                                        StructField ("lap",IntegerType(),False),
                                        StructField ("time",StringType(),False),
                                        StructField ("duration",FloatType(),False),
                                        StructField ("milliseconds",IntegerType(),False)
                                    
])

Pitstops_Df = spark.read\
            .schema(Pitstops_Schema)\
            .option("multiline",True)\
            .json(f"{raw_folder_path}/{V_Filedate}/pit_stops.json")

# COMMAND ----------

Pitstops_transform_df = Pitstops_Df.withColumnRenamed("raceId","race_Id")\
                                .withColumnRenamed("driverId","driver_Id")\
                                .withColumn("Field_date",lit(V_Filedate))\
                                .withColumn("Source",lit(V_Source))\
                                .withColumn("Ingestion_time",current_timestamp())

# COMMAND ----------

#Pitstops_transform_df.write.mode("overwrite").parquet("dbfs:/mnt/processed/pit_stops")

# COMMAND ----------

Merge_condition = 'tgt.driver_Id = src.driver_Id'
merge_delta_data(Pitstops_transform_df,'f1_processed','pitstops',processed_folder_path,Merge_condition,'race_Id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1) from f1_processed.pitstops
# MAGIC Group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

display(Pitstops_transform_df)

# COMMAND ----------

